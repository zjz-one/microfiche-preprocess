#!/usr/bin/env python3
from __future__ import annotations

import base64
import csv
import datetime as dt
import io
import json
import os
import queue
import re
import statistics
import threading
import time
import traceback
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlparse

import fitz  # PyMuPDF
import requests
import tkinter as tk
from PIL import Image, ImageEnhance, ImageOps, ImageStat
from tkinter import filedialog, font as tkfont, messagebox, simpledialog, ttk
from tkinter.scrolledtext import ScrolledText

APP_NAME = "Microfiche Problem Detector"
APP_VERSION = "0.3.1"

DEFAULT_CLASSIFY_PROMPT = (
    "Task: classify each microfiche page as overlap, blurry, clean, or uncertain.\n"
    "The supplied review image is a composite: the top panel is the full page, the bottom-left panel is an enlarged crop of the rightmost edge, "
    "the bottom-middle panel is a high-contrast enlarged version of that same right-edge crop, and the bottom-right panel is a high-contrast "
    "extreme-right micro-strip showing only the outermost sliver of the page.\n"
    "Definition of overlap: two different record cards are merged/superimposed in one scan, "
    "including clear side-by-side merge OR ghost/partial superimposition.\n"
    "Typical clean transcript card content is often around 1000:447 in horizontal-to-vertical proportion "
    "(about 2.24:1), but the exact clean ratio band for this batch is provided in the measured cues.\n"
    "The most important task is to identify the TRUE outer right boundary of the first/main transcript card. "
    "Do NOT use an internal grade-column divider or table separator as the first-page boundary.\n"
    "Use the provided clean-ratio band and ratio-guided boundary cues. "
    "If the full visible card already falls inside the clean ratio band, treat the full visible right edge as the page boundary; "
    "that means there is no boundary-overflow evidence.\n"
    "A second, weaker geometry cue is also provided: a correct black-edge-trimmed page-body ratio. "
    "This trimmed ratio is only weighting evidence and should not by itself force overlap, unless the trimmed page-body shape is directly distorted/irregular.\n"
    "Only use boundary overflow as overlap evidence when the ratio-guided boundary is reliable. "
    "Then ask whether there is still structured content to the right of that TRUE outer boundary: text, grades, headings, table lines, "
    "or card/frame fragments. The second card's left edge may be hidden, so do not require a visible second-card start.\n"
    "Very subtle overlaps may still fit the normal clean ratio band. In those cases, inspect the enlarged right-edge panels, especially the "
    "extreme-right micro-strip, for a narrow extra structured strip hugging the extreme right side: extra code text, frame fragments, a second "
    "boxed region, a second vertical content band, or a second outer border hugging the page edge.\n"
    "If the page looks materially wider/longer than a normal transcript card, or if content exists to the right of a reliable first-card boundary, "
    "do an OCR-style verification mentally: read for duplicated/conflicting names, grades, headings, or two superimposed text layers before returning clean.\n"
    "If boundary evidence is weak or unreliable, return uncertain unless the overlap is visually unmistakable from direct superimposed/ghost content.\n"
    "Definition of blurry: text is so unreadable that student name and grades cannot be recognized at all. "
    "If any student name or any grades can be seen even partially, it is NOT blurry.\n"
    "You must choose exactly one decision class.\n"
    "Return strict JSON only with keys:\n"
    "- decision: one of [overlap, blurry, clean, uncertain]\n"
    "- is_overlap: boolean\n"
    "- is_blurry: boolean\n"
    "- confidence: number in [0,1]\n"
    "- overlap_type: one of [clear_double_card, ghost_superimposition, none]\n"
    "- signatures: array of 0..2 concise identity strings (e.g. NAME|DOB|STUDENTNO)\n"
    "- reason: short string\n"
)

# Display name -> candidate real model IDs.
# The GUI only shows display names. Request routing uses this internal mapping.
MODEL_NAME_TO_IDS: Dict[str, List[str]] = {
    "GPT-5.4": ["gpt-5.4", "GPT-5.4"],
    "GPT-5.3-Codex": ["gpt-5.3-codex", "GPT-5.3-Codex"],
    "Claude-Opus-4.6": ["claude-opus-4-6", "claude-opus-4-5-20251101"],
    "Kimi-K2.5": ["kimi-k2.5"],
    "GLM-5": ["glm-5"],
    "MiniMax-M2.5": ["MiniMax-M2.5", "minimax-m2.5"],
}

UI_TOKENS: Dict[str, str] = {
    "canvas": "#F4F5F7",
    "canvas_soft": "#ECEFF2",
    "card": "#F9FAFB",
    "card_soft": "#F3F5F7",
    "card_strong": "#FFFFFF",
    "line": "#D7DDE3",
    "line_soft": "#E4E8EC",
    "ink": "#25303A",
    "ink_soft": "#5E6670",
    "muted": "#7A838D",
    "accent": "#8796A8",
    "accent_soft": "#E8EEF4",
    "rose_soft": "#F2E8EB",
    "ice_soft": "#E6EDF3",
    "sand_soft": "#EEECE8",
    "run": "#7B8A9B",
    "danger": "#B88282",
    "success": "#70866F",
}

BODY_DARK_THRESHOLD = 100
BODY_COVERAGE_FRAC = 0.30
BODY_IRREGULAR_SPREAD = 45
BODY_IRREGULAR_RIGHT_SHIFT = 35
DEFAULT_NORMAL_BODY_WIDTH = 1580.0
PY_WIDTH_OVERLAP_REL_THRESHOLD = 1.03


def resolve_model_candidates(display_model: str) -> List[str]:
    key = (display_model or "").strip()
    if not key:
        return []
    if key in MODEL_NAME_TO_IDS:
        return MODEL_NAME_TO_IDS[key][:]
    # case-insensitive fallback
    for k, ids in MODEL_NAME_TO_IDS.items():
        if k.lower() == key.lower():
            return ids[:]
    # custom model profile: use as-is
    return [key]


def normalize_display_model_name(name: str, model: str) -> str:
    n = (name or "").strip()
    m = (model or "").strip()
    # Exact match on display names.
    if n in MODEL_NAME_TO_IDS:
        return n
    if m in MODEL_NAME_TO_IDS:
        return m
    # Reverse mapping from actual model id to display name.
    for display, ids in MODEL_NAME_TO_IDS.items():
        for mid in ids:
            if mid.lower() == m.lower():
                return display
    # Legacy profile names created by earlier app versions.
    legacy = n.lower()
    if legacy.startswith("gpt-5.4"):
        return "GPT-5.4"
    if legacy.startswith("gpt-5.3"):
        return "GPT-5.3-Codex"
    if "claude-opus-4.6" in legacy or "claude-opus-4-6" in legacy:
        return "Claude-Opus-4.6"
    if "kimi-k2.5" in legacy:
        return "Kimi-K2.5"
    if legacy.startswith("glm-5"):
        return "GLM-5"
    if "minimax" in legacy and "2.5" in legacy:
        return "MiniMax-M2.5"
    return n or m


def now_ts() -> str:
    return dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def now_file_ts() -> str:
    return dt.datetime.now().strftime("%Y%m%d_%H%M%S")


def app_data_dir() -> Path:
    if os.name == "nt":
        base = Path(os.environ.get("APPDATA", str(Path.home())))
        root = base / "MicroficheProblemDetector"
    else:
        root = Path.home() / ".microfiche_problem_detector"
    root.mkdir(parents=True, exist_ok=True)
    return root


@dataclass
class ModelProfile:
    name: str
    base_url: str
    model: str
    api_key: str = ""
    timeout_sec: int = 120


class Storage:
    def __init__(self) -> None:
        self.root = app_data_dir()
        self.models_path = self.root / "models_config.json"
        self.memory_path = self.root / "memory_store.json"
        self.last_scan_path = self.root / "last_scan.json"

    def load_models(self) -> List[ModelProfile]:
        if not self.models_path.exists():
            models = self.default_models()
            self.save_models(models)
            return models
        try:
            raw = json.loads(self.models_path.read_text(encoding="utf-8"))
            out: List[ModelProfile] = []
            changed = False
            for x in raw:
                raw_name = str(x.get("name", ""))
                raw_model = str(x.get("model", ""))
                raw_name_l = raw_name.strip().lower()
                raw_model_l = raw_model.strip().lower()
                if "codex" in raw_name_l or "codex" in raw_model_l:
                    changed = True
                    continue
                display_model = normalize_display_model_name(raw_name, raw_model)
                if display_model in {"GPT-5.2"}:
                    changed = True
                    continue
                if display_model and (raw_name != display_model or raw_model != display_model):
                    changed = True
                out.append(
                    ModelProfile(
                        name=display_model,
                        base_url=str(x.get("base_url", "")),
                        model=display_model,
                        api_key=str(x.get("api_key", "")),
                        timeout_sec=int(x.get("timeout_sec", 120)),
                    )
                )
            defaults_by_name = {m.name: m for m in self.default_models()}
            if not any(m.name == "GPT-5.4" for m in out):
                out.insert(0, defaults_by_name["GPT-5.4"])
                changed = True
            if not any(m.name == "GPT-5.3-Codex" for m in out):
                out.insert(1 if out else 0, defaults_by_name["GPT-5.3-Codex"])
                changed = True
            if changed and out:
                self.save_models(out)
            return out or self.default_models()
        except Exception:
            return self.default_models()

    def save_models(self, models: List[ModelProfile]) -> None:
        self.models_path.write_text(
            json.dumps([asdict(m) for m in models], indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

    def load_memory(self) -> Dict[str, Any]:
        if not self.memory_path.exists():
            data = {"global_notes": [], "overrides": {}, "correction_history": []}
            self.save_memory(data)
            return data
        try:
            data = json.loads(self.memory_path.read_text(encoding="utf-8"))
            data.setdefault("global_notes", [])
            data.setdefault("overrides", {})
            data.setdefault("correction_history", [])
            return data
        except Exception:
            data = {"global_notes": [], "overrides": {}, "correction_history": []}
            self.save_memory(data)
            return data

    def save_memory(self, memory: Dict[str, Any]) -> None:
        self.memory_path.write_text(
            json.dumps(memory, indent=2, ensure_ascii=False), encoding="utf-8"
        )

    def save_last_scan(self, records: List[Dict[str, Any]]) -> None:
        self.last_scan_path.write_text(
            json.dumps(records, indent=2, ensure_ascii=False), encoding="utf-8"
        )

    def load_last_scan(self) -> List[Dict[str, Any]]:
        if not self.last_scan_path.exists():
            return []
        try:
            return json.loads(self.last_scan_path.read_text(encoding="utf-8"))
        except Exception:
            return []

    @staticmethod
    def default_models() -> List[ModelProfile]:
        return [
            ModelProfile(
                name="GPT-5.4",
                base_url="https://ai.last.ee",
                model="GPT-5.4",
                api_key="sk-9b06f0ac4851ba8cdef2498ba269978ae5c64e099720b2a1b32d0d1b5f6631b4",
                timeout_sec=120,
            ),
            ModelProfile(
                name="GPT-5.3-Codex",
                base_url="https://ai.last.ee",
                model="GPT-5.3-Codex",
                api_key="sk-9b06f0ac4851ba8cdef2498ba269978ae5c64e099720b2a1b32d0d1b5f6631b4",
                timeout_sec=120,
            ),
            ModelProfile(
                name="Claude-Opus-4.6",
                base_url="https://cursor.scihub.edu.kg/api/v1",
                model="Claude-Opus-4.6",
                api_key="cr_56c958bfb141949f0a7e3ce7bf9e83315fe7695edf95749683c05b234c594000",
                timeout_sec=150,
            ),
            ModelProfile(
                name="Kimi-K2.5",
                base_url="https://coding.dashscope.aliyuncs.com/v1",
                model="Kimi-K2.5",
                api_key="sk-sp-a745d056ce96479c899d2b5d9c40d345",
                timeout_sec=120,
            ),
            ModelProfile(
                name="GLM-5",
                base_url="https://coding.dashscope.aliyuncs.com/v1",
                model="GLM-5",
                api_key="sk-sp-a745d056ce96479c899d2b5d9c40d345",
                timeout_sec=120,
            ),
            ModelProfile(
                name="MiniMax-M2.5",
                base_url="https://coding.dashscope.aliyuncs.com/v1",
                model="MiniMax-M2.5",
                api_key="sk-sp-a745d056ce96479c899d2b5d9c40d345",
                timeout_sec=120,
            ),
        ]


def parse_json_object(text: str) -> Dict[str, Any]:
    text = text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
    m = re.search(r"\{[\s\S]*\}", text)
    if m:
        text = m.group(0)
    try:
        obj = json.loads(text)
        if isinstance(obj, dict):
            return obj
    except Exception:
        pass
    return {}


def norm_sig(sig: str) -> str:
    s = re.sub(r"[^A-Za-z0-9|]+", " ", sig.upper()).strip()
    s = re.sub(r"\s+", " ", s)
    return s


def sig_tokens(sig: str) -> set[str]:
    return set([x for x in re.split(r"[|\s]+", norm_sig(sig)) if x])


def jaccard(a: set[str], b: set[str]) -> float:
    if not a or not b:
        return 0.0
    inter = len(a & b)
    uni = len(a | b)
    return inter / uni if uni else 0.0


def list_pdfs(root: Path, recursive: bool) -> List[Path]:
    if recursive:
        return sorted([p for p in root.rglob("*.pdf") if p.is_file()])
    return sorted([p for p in root.glob("*.pdf") if p.is_file()])


def render_page_jpeg(page: fitz.Page, dpi: int = 220, max_width: Optional[int] = 960, quality: int = 55) -> bytes:
    pix = page.get_pixmap(dpi=dpi, colorspace=fitz.csRGB)
    img = Image.frombytes("RGB", (pix.width, pix.height), pix.samples)
    if max_width and img.width > max_width:
        h = int(img.height * max_width / img.width)
        img = img.resize((max_width, h), Image.Resampling.LANCZOS)
    bio = io.BytesIO()
    img.save(bio, format="JPEG", quality=quality, optimize=True)
    return bio.getvalue()


def resize_to_fit(img: Image.Image, max_size: Tuple[int, int]) -> Image.Image:
    out = img.copy()
    out.thumbnail(max_size, Image.Resampling.LANCZOS)
    return out


def build_llm_review_jpeg(
    image_jpeg: bytes,
    highres_right_strip_jpeg: Optional[bytes] = None,
    right_frac: float = 0.18,
    micro_frac: float = 0.06,
) -> bytes:
    src = Image.open(io.BytesIO(image_jpeg)).convert("RGB")
    full_w, full_h = src.size
    if highres_right_strip_jpeg:
        right_crop = Image.open(io.BytesIO(highres_right_strip_jpeg)).convert("RGB")
    else:
        crop_x = max(0, int(full_w * (1.0 - right_frac)))
        right_crop = src.crop((crop_x, 0, full_w, full_h))
    panel_gap = 16
    bottom_max_h = max(360, min(760, int(full_h * 1.75)))
    bottom_max_w = max(180, (full_w - panel_gap * 4) // 3)
    zoom_rgb = resize_to_fit(right_crop, (bottom_max_w, bottom_max_h))
    zoom_gray = ImageOps.grayscale(zoom_rgb)
    zoom_gray = ImageEnhance.Contrast(zoom_gray).enhance(2.4)
    zoom_gray = ImageEnhance.Sharpness(zoom_gray).enhance(1.6).convert("RGB")
    micro_ratio = max(0.18, min(0.7, micro_frac / max(right_frac, 0.001)))
    micro_x = max(0, int(right_crop.width * (1.0 - micro_ratio)))
    micro_crop = right_crop.crop((micro_x, 0, right_crop.width, right_crop.height))
    micro_zoom = resize_to_fit(micro_crop, (bottom_max_w, bottom_max_h))
    micro_gray = ImageOps.grayscale(micro_zoom)
    micro_gray = ImageEnhance.Contrast(micro_gray).enhance(2.8)
    micro_gray = ImageEnhance.Sharpness(micro_gray).enhance(1.9).convert("RGB")

    bottom_h = max(zoom_rgb.height, zoom_gray.height, micro_gray.height)
    canvas_h = full_h + bottom_h + panel_gap * 3
    canvas = Image.new("RGB", (full_w, canvas_h), color=(250, 250, 248))
    canvas.paste(src, (0, 0))

    y0 = full_h + panel_gap * 2
    panel_xs = [
        panel_gap,
        panel_gap * 2 + bottom_max_w,
        panel_gap * 3 + bottom_max_w * 2,
    ]
    canvas.paste(zoom_rgb, (panel_xs[0], y0))
    canvas.paste(zoom_gray, (panel_xs[1], y0))
    canvas.paste(micro_gray, (panel_xs[2], y0))

    divider = Image.new("RGB", (full_w, panel_gap), color=(210, 210, 206))
    canvas.paste(divider, (0, full_h + panel_gap))
    divider_v = Image.new("RGB", (2, bottom_h), color=(210, 210, 206))
    canvas.paste(divider_v, (panel_gap + bottom_max_w + (panel_gap // 2), y0))
    canvas.paste(divider_v, (panel_gap * 2 + bottom_max_w * 2 + (panel_gap // 2), y0))

    bio = io.BytesIO()
    canvas.save(bio, format="JPEG", quality=78, optimize=True)
    return bio.getvalue()


def build_extreme_right_microstrip_jpeg(
    image_jpeg: bytes,
    highres_right_strip_jpeg: Optional[bytes] = None,
    right_frac: float = 0.18,
    micro_frac: float = 0.06,
) -> bytes:
    src = Image.open(io.BytesIO(image_jpeg)).convert("RGB")
    if highres_right_strip_jpeg:
        right_crop = Image.open(io.BytesIO(highres_right_strip_jpeg)).convert("RGB")
    else:
        crop_x = max(0, int(src.width * (1.0 - right_frac)))
        right_crop = src.crop((crop_x, 0, src.width, src.height))
    micro_ratio = max(0.18, min(0.7, micro_frac / max(right_frac, 0.001)))
    micro_x = max(0, int(right_crop.width * (1.0 - micro_ratio)))
    micro_crop = right_crop.crop((micro_x, 0, right_crop.width, right_crop.height))
    micro_crop = resize_to_fit(micro_crop, (360, 1000))
    micro_gray = ImageOps.grayscale(micro_crop)
    micro_gray = ImageEnhance.Contrast(micro_gray).enhance(2.8)
    micro_gray = ImageEnhance.Sharpness(micro_gray).enhance(1.9)
    bio = io.BytesIO()
    micro_gray.save(bio, format="JPEG", quality=84, optimize=True)
    return bio.getvalue()


def compute_right_strip_stats(gray_img: Image.Image) -> Dict[str, Any]:
    w, h = gray_img.size
    cues: Dict[str, Any] = {}
    for frac, key in ((0.06, "6"), (0.08, "8"), (0.10, "10"), (0.12, "12")):
        x0 = max(0, int(w * (1.0 - frac)))
        strip = gray_img.crop((x0, 0, w, h))
        strip_mask = strip.point(lambda px: 255 if px < 205 else 0, mode="L")
        strip_w, strip_h = strip_mask.size
        strip_pixels = strip_mask.load()
        strip_dark = 0
        strip_cols: List[int] = []
        for sx in range(strip_w):
            col_dark = 0
            for sy in range(strip_h):
                if strip_pixels[sx, sy]:
                    col_dark += 1
                    strip_dark += 1
            strip_cols.append(col_dark)
        strip_area = max(1, strip_w * strip_h)
        strip_peak_cols = sum(1 for v in strip_cols if v >= strip_h * 0.35)
        strip_max_col_ratio = (max(strip_cols) / max(strip_h, 1)) if strip_cols else 0.0
        cues[f"right_strip_dark_ratio_{key}"] = round(strip_dark / strip_area, 4)
        cues[f"right_strip_peak_cols_{key}"] = int(strip_peak_cols)
        cues[f"right_strip_maxcol_ratio_{key}"] = round(strip_max_col_ratio, 4)
    return cues


def subtle_right_strip_flag(cues: Dict[str, Any], content_ratio_in_band: bool) -> bool:
    if not content_ratio_in_band:
        return False
    return bool(
        (
            float(cues.get("right_strip_dark_ratio_6", 0.0)) >= 0.66
            and int(cues.get("right_strip_peak_cols_6", 0)) >= 150
            and float(cues.get("right_strip_maxcol_ratio_6", 0.0)) <= 0.93
        )
        or (
            float(cues.get("right_strip_dark_ratio_8", 0.0)) >= 0.62
            and int(cues.get("right_strip_peak_cols_8", 0)) >= 150
            and float(cues.get("right_strip_maxcol_ratio_8", 0.0)) <= 0.93
        )
        or (
            float(cues.get("right_strip_dark_ratio_10", 0.0)) >= 0.70
            and int(cues.get("right_strip_peak_cols_10", 0)) >= 210
            and float(cues.get("right_strip_maxcol_ratio_10", 0.0)) <= 0.93
        )
    )


def compute_page_body_bbox(
    gray_img: Image.Image,
    dark_threshold: int = BODY_DARK_THRESHOLD,
    coverage_frac: float = BODY_COVERAGE_FRAC,
) -> Optional[Tuple[int, int, int, int]]:
    w, h = gray_img.size
    if w <= 0 or h <= 0:
        return None
    pix = gray_img.load()
    rows: List[float] = []
    for y in range(h):
        bright = 0
        for x in range(w):
            if pix[x, y] > dark_threshold:
                bright += 1
        rows.append(bright / max(w, 1))
    cols: List[float] = []
    for x in range(w):
        bright = 0
        for y in range(h):
            if pix[x, y] > dark_threshold:
                bright += 1
        cols.append(bright / max(h, 1))
    ys = [i for i, frac in enumerate(rows) if frac >= coverage_frac]
    xs = [i for i, frac in enumerate(cols) if frac >= coverage_frac]
    if not xs or not ys:
        return None
    return int(min(xs)), int(min(ys)), int(max(xs) + 1), int(max(ys) + 1)


def estimate_trimmed_body_width(
    image_jpeg: bytes,
    dark_threshold: int = BODY_DARK_THRESHOLD,
    coverage_frac: float = BODY_COVERAGE_FRAC,
) -> float:
    try:
        gray = Image.open(io.BytesIO(image_jpeg)).convert("L")
        bbox = compute_page_body_bbox(gray, dark_threshold=dark_threshold, coverage_frac=coverage_frac)
        if not bbox:
            return 0.0
        return float(max(1, bbox[2] - bbox[0]))
    except Exception:
        return 0.0


def render_highres_right_strip_jpeg(page: fitz.Page, right_frac: float = 0.18, dpi: int = 360, quality: int = 84) -> Tuple[bytes, Dict[str, Any]]:
    pix = page.get_pixmap(dpi=dpi, colorspace=fitz.csRGB)
    img = Image.frombytes("RGB", (pix.width, pix.height), pix.samples)
    gray = ImageOps.grayscale(img)
    cues = compute_right_strip_stats(gray)
    crop_x = max(0, int(img.width * (1.0 - right_frac)))
    right_crop = img.crop((crop_x, 0, img.width, img.height))
    right_crop = resize_to_fit(right_crop, (520, 880))
    bio = io.BytesIO()
    right_crop.save(bio, format="JPEG", quality=quality, optimize=True)
    return bio.getvalue(), cues


def is_localish_base_url(base_url: str) -> bool:
    raw = (base_url or "").strip()
    if not raw:
        return False
    if "://" not in raw:
        raw = "http://" + raw
    try:
        parsed = urlparse(raw)
    except Exception:
        return False
    host = (parsed.hostname or "").strip().lower()
    if not host:
        return False
    return host in {"127.0.0.1", "localhost", "0.0.0.0", "::1"} or host.endswith(".local")


def is_azure_openai_base_url(base_url: str) -> bool:
    raw = (base_url or "").strip()
    if not raw:
        return False
    if "://" not in raw:
        raw = "https://" + raw
    try:
        parsed = urlparse(raw)
    except Exception:
        return False
    host = (parsed.hostname or "").strip().lower()
    return host.endswith(".openai.azure.com") or host.endswith(".openai.azure.us")


def looks_like_bearer_token(secret: str) -> bool:
    s = (secret or "").strip()
    if not s:
        return False
    if s.lower().startswith("bearer "):
        return True
    return s.count(".") >= 2 and len(s) > 80


def estimate_width_baseline(
    pdf_paths: List[Path], dpi: int = 110, max_width: int = 640, quality: int = 40
) -> Dict[str, Any]:
    widths: List[float] = []
    page_count = 0
    for pdf_path in pdf_paths:
        try:
            doc = fitz.open(str(pdf_path))
        except Exception:
            continue
        try:
            for idx in range(len(doc)):
                try:
                    image_jpeg = render_page_jpeg(doc[idx], dpi=dpi, max_width=max_width, quality=quality)
                    width = estimate_trimmed_body_width(image_jpeg)
                    if width > 0:
                        widths.append(width)
                    if width > 0:
                        page_count += 1
                except Exception:
                    continue
        finally:
            doc.close()

    if not widths:
        return {
            "baseline_body_width": 0.0,
            "body_width_overlap_rel_threshold": PY_WIDTH_OVERLAP_REL_THRESHOLD,
            "body_width_overlap_threshold": 0.0,
            "page_count": 0,
            "body_width_median": 0.0,
            "body_width_min": 0.0,
            "body_width_max": 0.0,
        }

    widths_sorted = sorted(widths)
    width_cluster_end = max(1, int(round(len(widths_sorted) * 0.70)))
    width_cluster = widths_sorted[:width_cluster_end]
    width_baseline = float(statistics.median(width_cluster))
    width_overlap_threshold = round(width_baseline * PY_WIDTH_OVERLAP_REL_THRESHOLD, 3)
    width_median = round(statistics.median(widths_sorted), 3)
    width_min = round(widths_sorted[0], 3)
    width_max = round(widths_sorted[-1], 3)
    return {
        "baseline_body_width": round(width_baseline, 3) if width_baseline else 0.0,
        "body_width_overlap_rel_threshold": PY_WIDTH_OVERLAP_REL_THRESHOLD,
        "body_width_overlap_threshold": width_overlap_threshold,
        "body_width_median": width_median,
        "body_width_min": width_min,
        "body_width_max": width_max,
        "page_count": page_count,
    }


def measure_page_visual_cues(image_jpeg: bytes) -> Dict[str, Any]:
    try:
        img = Image.open(io.BytesIO(image_jpeg)).convert("L")
        w, h = img.size
        cues: Dict[str, Any] = {
            "image_width": w,
            "image_height": h,
            "image_ratio": round(w / max(h, 1), 3),
        }
        body_bbox = compute_page_body_bbox(
            img,
            dark_threshold=BODY_DARK_THRESHOLD,
            coverage_frac=BODY_COVERAGE_FRAC,
        )
        if body_bbox:
            bx0, by0, bx1, by1 = body_bbox
            body_w = max(1, bx1 - bx0)
            body_h = max(1, by1 - by0)
            cues.update(
                {
                    "trimmed_body_bbox": [int(x) for x in body_bbox],
                    "trimmed_body_width": int(body_w),
                    "trimmed_body_height": int(body_h),
                }
            )
        else:
            cues.update(
                {
                    "trimmed_body_bbox": "",
                    "trimmed_body_width": "",
                    "trimmed_body_height": "",
                }
            )
        return cues
    except Exception:
        return {}


def enrich_python_width_cues(page_cues: Dict[str, Any], batch_info: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    page_cues = dict(page_cues or {})
    batch_info = batch_info or {}
    body_width = float(page_cues.get("trimmed_body_width") or 0.0)
    baseline_width = float(batch_info.get("baseline_body_width") or 0.0)
    rel_threshold = float(batch_info.get("body_width_overlap_rel_threshold") or PY_WIDTH_OVERLAP_REL_THRESHOLD)
    abs_threshold = float(batch_info.get("body_width_overlap_threshold") or 0.0)
    rel_width = 0.0
    if body_width > 0 and baseline_width > 0:
        rel_width = body_width / baseline_width
    page_cues.update(
        {
            "trimmed_body_rel_width": round(rel_width, 4) if rel_width else 0.0,
            "trimmed_body_width_overlap_rel_threshold": rel_threshold,
            "trimmed_body_width_overlap_threshold": round(abs_threshold, 3) if abs_threshold else 0.0,
            "trimmed_body_width_overlap_hint": bool(rel_width > rel_threshold) if rel_width else False,
        }
    )
    return page_cues


def compute_blurry_stats(image_jpeg: bytes) -> Dict[str, Any]:
    try:
        gray = Image.open(io.BytesIO(image_jpeg)).convert("L")
        stats_img = resize_to_fit(gray, (260, 1600))
        stat = ImageStat.Stat(stats_img)
        mean_luma = float(stat.mean[0]) if stat.mean else 0.0
        contrast_stddev = float(stat.stddev[0]) if stat.stddev else 0.0

        hist = stats_img.histogram()
        total = max(1, sum(hist))
        dark_ratio = sum(hist[:170]) / total

        edge_img = resize_to_fit(gray, (180, 1200))
        ew, eh = edge_img.size
        pix = edge_img.load()
        edge_sum = 0
        edge_count = 0
        for y in range(1, eh):
            for x in range(1, ew):
                edge_sum += abs(pix[x, y] - pix[x - 1, y])
                edge_sum += abs(pix[x, y] - pix[x, y - 1])
                edge_count += 2
        edge_energy = edge_sum / max(edge_count, 1)

        mask = stats_img.point(lambda px: 255 if px < 205 else 0, mode="L")
        bbox = mask.getbbox()
        if bbox:
            bw = max(1, bbox[2] - bbox[0])
            bh = max(1, bbox[3] - bbox[1])
            bbox_area = bw * bh
            dark_bbox = 0
            mpix = mask.load()
            for y in range(bbox[1], bbox[3]):
                for x in range(bbox[0], bbox[2]):
                    if mpix[x, y]:
                        dark_bbox += 1
            content_fill_ratio = dark_bbox / max(bbox_area, 1)
        else:
            content_fill_ratio = 0.0

        return {
            "blur_mean_luma": round(mean_luma, 3),
            "blur_contrast_stddev": round(contrast_stddev, 3),
            "blur_dark_ratio": round(dark_ratio, 4),
            "blur_edge_energy": round(edge_energy, 4),
            "blur_content_fill_ratio": round(content_fill_ratio, 4),
        }
    except Exception:
        return {}


def classify_python_page(page_cues: Dict[str, Any], blurry_stats: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    blurry_stats = blurry_stats or {}
    trimmed_body_width = float(page_cues.get("trimmed_body_width") or 0.0)
    trimmed_body_rel_width = float(page_cues.get("trimmed_body_rel_width") or 0.0)
    trimmed_body_width_overlap_hint = bool(page_cues.get("trimmed_body_width_overlap_hint"))
    trimmed_body_width_overlap_threshold = float(page_cues.get("trimmed_body_width_overlap_threshold") or 0.0)
    trimmed_body_width_overlap_rel_threshold = float(page_cues.get("trimmed_body_width_overlap_rel_threshold") or PY_WIDTH_OVERLAP_REL_THRESHOLD)

    blur_contrast = float(blurry_stats.get("blur_contrast_stddev") or 0.0)
    blur_edge = float(blurry_stats.get("blur_edge_energy") or 0.0)
    blur_dark = float(blurry_stats.get("blur_dark_ratio") or 0.0)
    blur_fill = float(blurry_stats.get("blur_content_fill_ratio") or 0.0)

    if trimmed_body_width_overlap_hint:
        width_bonus = max(0.0, trimmed_body_rel_width - trimmed_body_width_overlap_rel_threshold)
        confidence = min(0.98, 0.84 + min(width_bonus * 5.0, 0.12))
        return {
            "decision": "overlap",
            "is_overlap": True,
            "is_blurry": False,
            "confidence": round(confidence, 3),
            "overlap_type": "clear_double_card",
            "signatures": [],
            "reason": "python_rule: correct black-edge-trimmed page-body width exceeds the overlap threshold",
        }

    blurry_strong = bool(
        blur_edge <= 7.2
        and blur_contrast <= 17.0
        and blur_dark <= 0.12
        and blur_fill <= 0.16
        and not trimmed_body_width_overlap_hint
    )
    blurry_possible = bool(
        blur_edge <= 8.4
        and blur_contrast <= 20.0
        and blur_dark <= 0.14
        and blur_fill <= 0.20
        and not trimmed_body_width_overlap_hint
    )

    if blurry_strong:
        confidence = min(0.90, 0.78 + max(0.0, (8.0 - blur_edge) * 0.02))
        return {
            "decision": "blurry",
            "is_overlap": False,
            "is_blurry": True,
            "confidence": round(confidence, 3),
            "overlap_type": "none",
            "signatures": [],
            "reason": "python_rule: very low edge detail and contrast indicate unreadable page content",
        }

    if blurry_possible:
        return {
            "decision": "uncertain",
            "is_overlap": False,
            "is_blurry": False,
            "confidence": 0.42,
            "overlap_type": "none",
            "signatures": [],
            "reason": "python_rule: page may be blurry, but the image-quality cues are not strong enough for a hard blurry call",
        }

    return {
        "decision": "clean",
        "is_overlap": False,
        "is_blurry": False,
        "confidence": 0.93,
        "overlap_type": "none",
        "signatures": [],
        "reason": "python_rule: correct black-edge-trimmed page-body width is inside the clean range",
    }


OVERLAP_CSV_FIELDS = [
    "source_directory",
    "file_name",
    "file_path",
    "page",
    "decision",
    "is_overlap",
    "is_blurry",
    "confidence",
    "overlap_type",
    "signatures",
    "reason",
    "status",
    "error_detail",
    "trimmed_body_width",
]


def overlap_row_for_csv(rec: Dict[str, Any]) -> Dict[str, Any]:
    row = dict(rec)
    row["signatures"] = " | ".join(rec.get("signatures", []))
    return {k: row.get(k, "") for k in OVERLAP_CSV_FIELDS}


def normalize_decision_fields(obj: Dict[str, Any]) -> Tuple[str, bool, bool]:
    decision = str(obj.get("decision", "")).strip().lower()
    is_overlap = bool(obj.get("is_overlap", False))
    is_blurry = bool(obj.get("is_blurry", False))

    if decision not in {"overlap", "blurry", "clean", "uncertain"}:
        if is_overlap:
            decision = "overlap"
        elif is_blurry:
            decision = "blurry"
        else:
            decision = "clean"

    if decision == "overlap":
        return decision, True, False
    if decision == "blurry":
        return decision, False, True
    if decision == "clean":
        return decision, False, False
    return "uncertain", False, False


def apply_ratio_boundary_guard(
    decision: str, is_overlap: bool, is_blurry: bool, overlap_type: str, confidence: float, page_cues: Optional[Dict[str, Any]]
) -> Tuple[str, bool, bool, Optional[str]]:
    if decision != "overlap" or not page_cues:
        return decision, is_overlap, is_blurry, None

    overlap_type_norm = str(overlap_type or "none").strip().lower()
    confidence = float(confidence or 0.0)
    in_band = bool(page_cues.get("content_ratio_in_clean_band"))
    boundary_method = str(page_cues.get("boundary_method") or "")
    boundary_reliable = bool(page_cues.get("ratio_guided_boundary_reliable"))
    outside_structured = bool(page_cues.get("outside_structured_content"))
    subtle_right_strip_suspect = bool(page_cues.get("subtle_right_strip_suspect"))
    trimmed_body_irregular_overlap = bool(page_cues.get("trimmed_body_irregular_overlap"))
    trimmed_body_wide_hint = bool(page_cues.get("trimmed_body_wide_hint"))

    if (
        in_band
        and boundary_method == "bbox_right_edge"
        and overlap_type_norm != "ghost_superimposition"
        and not subtle_right_strip_suspect
        and not trimmed_body_irregular_overlap
        and not trimmed_body_wide_hint
    ):
        return (
            "uncertain",
            False,
            False,
            "ratio_guard: page width is inside clean band, full visible right edge is the boundary, and there is no boundary-overflow evidence",
        )

    if not boundary_reliable and overlap_type_norm != "ghost_superimposition" and confidence < 0.9:
        return (
            "uncertain",
            False,
            False,
            "ratio_guard: ratio-guided first-page boundary is not reliable, so overlap cannot be confirmed from boundary evidence",
        )

    if boundary_reliable and not outside_structured and overlap_type_norm != "ghost_superimposition" and confidence < 0.9:
        return (
            "uncertain",
            False,
            False,
            "ratio_guard: reliable first-page boundary found but no structured content exists beyond it",
        )

    return decision, is_overlap, is_blurry, None


def summarize_page_result(rec: Dict[str, Any]) -> str:
    return (
        f"{rec.get('file_name')} p{int(rec.get('page', 0)):03d}: "
        f"decision={rec.get('decision')} "
        f"reason={str(rec.get('reason', ''))[:120]}"
    )


def ensure_memory_schema(memory: Dict[str, Any]) -> Dict[str, Any]:
    memory.setdefault("global_notes", [])
    memory.setdefault("overrides", {})
    memory.setdefault("correction_history", [])
    return memory


def flags_from_decision(decision: str) -> Tuple[bool, bool]:
    d = (decision or "").strip().lower()
    return d == "overlap", d == "blurry"


def correction_summary(entry: Dict[str, Any]) -> str:
    file_name = str(entry.get("file_name", "unknown.pdf"))
    page = int(entry.get("page", 0))
    previous = str(entry.get("previous_decision", "unknown"))
    corrected = str(entry.get("corrected_decision", "unknown"))
    note = str(entry.get("note", "")).strip()
    bits = [f"{file_name} p{page}: corrected {previous} -> {corrected}"]
    if note:
        bits.append(f"note={note}")
    sigs = [str(s).strip() for s in entry.get("signatures", []) if str(s).strip()]
    if sigs:
        bits.append("signatures=" + " | ".join(sigs[:2]))
    return "; ".join(bits)


def build_memory_notes(memory: Dict[str, Any], file_name: str) -> List[str]:
    ensure_memory_schema(memory)
    notes: List[str] = []
    seen: set[str] = set()

    def add(note: str) -> None:
        note = note.strip()
        if not note:
            return
        if note in seen:
            return
        seen.add(note)
        notes.append(note)

    for note in memory.get("global_notes", [])[:10]:
        add(str(note))

    target = (file_name or "").strip().lower()
    history = [x for x in memory.get("correction_history", []) if isinstance(x, dict)]
    same_file = [x for x in reversed(history) if str(x.get("file_name", "")).strip().lower() == target]
    recent = list(reversed(history))

    for entry in same_file[:8]:
        add("Same-file correction memory: " + correction_summary(entry))
    for entry in recent[:8]:
        add("Recent correction memory: " + correction_summary(entry))

    return notes[:18]


def remember_page_correction(memory: Dict[str, Any], rec: Dict[str, Any], corrected_decision: str, note: str) -> Dict[str, Any]:
    ensure_memory_schema(memory)
    corrected_decision = corrected_decision.strip().lower()
    if corrected_decision not in {"overlap", "blurry", "clean", "uncertain"}:
        raise ValueError("Corrected decision must be one of overlap, blurry, clean, uncertain.")

    file_name = str(rec.get("file_name", "")).strip()
    page_no = int(rec.get("page", 0))
    if not file_name or page_no <= 0:
        raise ValueError("Correction target is missing file_name or page.")

    is_overlap, is_blurry = flags_from_decision(corrected_decision)
    key = f"{file_name.lower()}::{page_no}"
    override = {
        "decision": corrected_decision,
        "is_overlap": is_overlap,
        "is_blurry": is_blurry,
        "confidence": 1.0,
        "overlap_type": "manual_override",
        "signatures": list(rec.get("signatures", []))[:2],
        "note": note.strip() or f"manual correction from {rec.get('decision', 'unknown')} to {corrected_decision}",
        "updated_at": now_ts(),
    }
    memory["overrides"][key] = override

    history = memory.setdefault("correction_history", [])
    history.append(
        {
            "file_name": file_name,
            "file_path": str(rec.get("file_path", "")),
            "page": page_no,
            "previous_decision": str(rec.get("decision", "unknown")),
            "corrected_decision": corrected_decision,
            "note": note.strip(),
            "signatures": list(rec.get("signatures", []))[:2],
            "overlap_type": str(rec.get("overlap_type", "none")),
            "updated_at": now_ts(),
        }
    )
    if len(history) > 300:
        del history[:-300]

    global_notes = memory.setdefault("global_notes", [])
    if note and note.strip() and note.strip() not in global_notes:
        global_notes.append(note.strip())
    auto_note = (
        f"If a microfiche page looks unusually wide or stretched compared with a normal transcript card, "
        f"do not mark it clean until OCR-style reading rules out overlap. Example memory: {file_name} p{page_no} -> {corrected_decision}."
    )
    if auto_note not in global_notes:
        global_notes.append(auto_note)

    return override


def find_last_scan_record(records: List[Dict[str, Any]], file_ref: str, page_no: int) -> Optional[Dict[str, Any]]:
    file_ref = file_ref.strip().lower()
    candidates = []
    for rec in records:
        rec_file = str(rec.get("file_name", "")).strip().lower()
        rec_path = str(rec.get("file_path", "")).strip().lower()
        rec_page = int(rec.get("page", 0))
        if rec_page != page_no:
            continue
        if file_ref in {rec_file, rec_path, Path(rec_path).stem.lower(), Path(rec_path).name.lower()}:
            return rec
        if file_ref and (file_ref in rec_file or file_ref in rec_path):
            candidates.append(rec)
    if len(candidates) == 1:
        return candidates[0]
    return None


class CorrectionPicker(tk.Toplevel):
    def __init__(self, parent: "App", records: List[Dict[str, Any]]) -> None:
        super().__init__(parent)
        self.parent = parent
        self.records = [r for r in records if r.get("scope") == "source"]
        self.filtered_records = list(self.records)
        self.title("Correct Last Scan Page")
        self.geometry("1080x720")
        self.minsize(940, 620)
        self.configure(bg=parent.ui["canvas"])
        self.transient(parent)
        self.grab_set()

        self.filter_var = tk.StringVar()
        self.corrected_var = tk.StringVar(value="overlap")
        self.status_var = tk.StringVar(value="Select a page, then save a correction into local memory.")

        shell = tk.Frame(self, bg=parent.ui["canvas"])
        shell.pack(fill=tk.BOTH, expand=True, padx=18, pady=18)
        shell.columnconfigure(0, weight=1)
        shell.rowconfigure(1, weight=1)

        top = tk.Frame(shell, bg=parent.ui["card"])
        top.grid(row=0, column=0, sticky="ew", pady=(0, 12))
        tk.Label(top, text="Correct Last Scan Page", font=parent.font_heading, fg=parent.ui["ink"], bg=parent.ui["card"]).grid(row=0, column=0, sticky="w", padx=16, pady=(14, 4))
        tk.Label(
            top,
            text="Filter by file name, page, decision, or reason. Corrections are stored locally and reused in later scans.",
            font=parent.font_caption,
            fg=parent.ui["muted"],
            bg=parent.ui["card"],
        ).grid(row=1, column=0, sticky="w", padx=16, pady=(0, 14))

        filter_row = tk.Frame(shell, bg=parent.ui["canvas"])
        filter_row.grid(row=1, column=0, sticky="nsew")
        filter_row.columnconfigure(0, weight=3)
        filter_row.columnconfigure(1, weight=2)
        filter_row.rowconfigure(0, weight=1)

        left = tk.Frame(filter_row, bg=parent.ui["card"], highlightbackground=parent.ui["line"], highlightthickness=1, bd=0)
        left.grid(row=0, column=0, sticky="nsew", padx=(0, 10))
        left.columnconfigure(0, weight=1)
        left.rowconfigure(1, weight=1)

        search_row = tk.Frame(left, bg=parent.ui["card"])
        search_row.grid(row=0, column=0, sticky="ew", padx=14, pady=14)
        ttk.Entry(search_row, textvariable=self.filter_var, style="Glass.TEntry").pack(side=tk.LEFT, fill=tk.X, expand=True)
        ttk.Button(search_row, text="Filter", style="Glass.TButton", command=self.refresh).pack(side=tk.LEFT, padx=(8, 0))
        ttk.Button(search_row, text="Reset", style="Glass.TButton", command=self.reset_filter).pack(side=tk.LEFT, padx=(8, 0))
        self.filter_var.trace_add("write", lambda *_args: self.refresh())

        columns = ("file_name", "page", "decision", "confidence", "reason")
        self.tree = ttk.Treeview(left, columns=columns, show="headings", height=18)
        self.tree.grid(row=1, column=0, sticky="nsew", padx=14, pady=(0, 14))
        self.tree.heading("file_name", text="File")
        self.tree.heading("page", text="Page")
        self.tree.heading("decision", text="Current")
        self.tree.heading("confidence", text="Conf")
        self.tree.heading("reason", text="Reason")
        self.tree.column("file_name", width=240, anchor="w")
        self.tree.column("page", width=64, anchor="center")
        self.tree.column("decision", width=92, anchor="center")
        self.tree.column("confidence", width=60, anchor="center")
        self.tree.column("reason", width=360, anchor="w")
        self.tree.bind("<<TreeviewSelect>>", lambda _e: self.on_select())
        self.tree.bind("<Double-1>", lambda _e: self.save())

        right = tk.Frame(filter_row, bg=parent.ui["card"], highlightbackground=parent.ui["line"], highlightthickness=1, bd=0)
        right.grid(row=0, column=1, sticky="nsew")
        right.columnconfigure(0, weight=1)
        right.rowconfigure(4, weight=1)

        tk.Label(right, text="Selected Page", font=parent.font_heading, fg=parent.ui["ink"], bg=parent.ui["card"]).grid(row=0, column=0, sticky="w", padx=16, pady=(14, 8))
        self.selection_label = tk.Label(
            right,
            text="No page selected",
            font=parent.font_status,
            fg=parent.ui["ink_soft"],
            bg=parent.ui["card"],
            justify="left",
            wraplength=320,
        )
        self.selection_label.grid(row=1, column=0, sticky="w", padx=16)

        label_row = tk.Frame(right, bg=parent.ui["card"])
        label_row.grid(row=2, column=0, sticky="w", padx=16, pady=(14, 10))
        tk.Label(label_row, text="Corrected Label", font=parent.font_label, fg=parent.ui["ink_soft"], bg=parent.ui["card"]).pack(anchor="w")
        for val in ("overlap", "blurry", "clean", "uncertain"):
            tk.Radiobutton(
                label_row,
                text=val,
                value=val,
                variable=self.corrected_var,
                bg=parent.ui["card"],
                activebackground=parent.ui["card"],
                highlightthickness=0,
                fg=parent.ui["ink"],
                selectcolor=parent.ui["card_strong"],
            ).pack(anchor="w")

        tk.Label(right, text="Memory Note", font=parent.font_label, fg=parent.ui["ink_soft"], bg=parent.ui["card"]).grid(row=3, column=0, sticky="w", padx=16)
        self.note_text = ScrolledText(right, height=10)
        self.note_text.grid(row=4, column=0, sticky="nsew", padx=16, pady=(6, 10))
        parent._style_text_widget(self.note_text)

        bottom = tk.Frame(right, bg=parent.ui["card"])
        bottom.grid(row=5, column=0, sticky="ew", padx=16, pady=(0, 14))
        ttk.Button(bottom, text="Save Correction", style="Accent.TButton", command=self.save).pack(side=tk.LEFT)
        ttk.Button(bottom, text="Close", style="Glass.TButton", command=self.destroy).pack(side=tk.LEFT, padx=(8, 0))
        tk.Label(bottom, textvariable=self.status_var, font=parent.font_caption, fg=parent.ui["muted"], bg=parent.ui["card"]).pack(side=tk.RIGHT)

        self.refresh()

    def reset_filter(self) -> None:
        self.filter_var.set("")
        self.refresh()

    def refresh(self) -> None:
        query = self.filter_var.get().strip().lower()
        if query:
            self.filtered_records = []
            for rec in self.records:
                hay = " ".join(
                    [
                        str(rec.get("file_name", "")),
                        str(rec.get("file_path", "")),
                        str(rec.get("page", "")),
                        str(rec.get("decision", "")),
                        str(rec.get("reason", "")),
                    ]
                ).lower()
                if query in hay:
                    self.filtered_records.append(rec)
        else:
            self.filtered_records = list(self.records)

        for item in self.tree.get_children():
            self.tree.delete(item)
        for idx, rec in enumerate(self.filtered_records):
            self.tree.insert(
                "",
                tk.END,
                iid=str(idx),
                values=(
                    rec.get("file_name", ""),
                    int(rec.get("page", 0)),
                    rec.get("decision", ""),
                    f"{float(rec.get('confidence', 0.0)):.2f}",
                    str(rec.get("reason", ""))[:80],
                ),
            )
        self.status_var.set(f"{len(self.filtered_records)} pages shown")

    def selected_record(self) -> Optional[Dict[str, Any]]:
        sel = self.tree.selection()
        if not sel:
            return None
        idx = int(sel[0])
        if 0 <= idx < len(self.filtered_records):
            return self.filtered_records[idx]
        return None

    def on_select(self) -> None:
        rec = self.selected_record()
        if not rec:
            self.selection_label.configure(text="No page selected")
            return
        self.selection_label.configure(
            text=(
                f"{rec.get('file_name')} p{int(rec.get('page', 0)):03d}\n"
                f"Current: {rec.get('decision')} | Conf: {float(rec.get('confidence', 0.0)):.2f}\n"
                f"Endpoint: {rec.get('endpoint', '') or '-'}"
            )
        )
        self.corrected_var.set(str(rec.get("decision", "clean")))
        self.note_text.delete("1.0", tk.END)
        self.note_text.insert("1.0", str(rec.get("reason", "")))

    def save(self) -> None:
        rec = self.selected_record()
        if not rec:
            messagebox.showerror("No Selection", "Select a page first.", parent=self)
            return
        corrected = self.corrected_var.get().strip().lower()
        note = self.note_text.get("1.0", tk.END).strip()
        try:
            remember_page_correction(self.parent.memory, rec, corrected, note)
            self.parent.storage.save_memory(self.parent.memory)
            self.parent._refresh_memory_info()
            self.parent.log(
                f"Stored correction memory for {rec.get('file_name')} p{int(rec.get('page', 0)):03d}: "
                f"{rec.get('decision')} -> {corrected}"
            )
            self.status_var.set("Correction saved to local memory")
            messagebox.showinfo(
                "Correction Saved",
                f"Saved correction for {rec.get('file_name')} p{int(rec.get('page', 0))}: "
                f"{rec.get('decision')} -> {corrected}",
                parent=self,
            )
        except Exception as exc:
            messagebox.showerror("Correction Error", str(exc), parent=self)


class OpenAICompatibleClient:
    def __init__(self, profile: ModelProfile):
        self.profile = profile
        alias = profile.model.strip() or profile.name.strip()
        self.model_candidates = resolve_model_candidates(alias)
        base = (profile.base_url or "").lower()
        self.responses_only = "ai.last.ee" in base

    def _headers(self) -> Dict[str, str]:
        headers = {"Content-Type": "application/json"}
        secret = self.profile.api_key.strip()
        if not secret:
            return headers
        if is_azure_openai_base_url(self.profile.base_url):
            if looks_like_bearer_token(secret):
                token = secret[7:].strip() if secret.lower().startswith("bearer ") else secret
                headers["Authorization"] = f"Bearer {token}"
            else:
                headers["api-key"] = secret
        else:
            token = secret[7:].strip() if secret.lower().startswith("bearer ") else secret
            headers["Authorization"] = f"Bearer {token}"
        return headers

    def _post_json(self, endpoint: str, payload: Dict[str, Any]) -> Tuple[int, Dict[str, Any], str]:
        base = self.profile.base_url.rstrip("/")
        parsed = urlparse(base)
        if parsed.path.rstrip("/") == "":
            url = base + "/v1" + endpoint
        else:
            url = base + endpoint
        headers = self._headers()
        try:
            resp = requests.post(
                url,
                headers=headers,
                json=payload,
                timeout=self.profile.timeout_sec,
            )
            txt = resp.text
            try:
                obj = resp.json()
            except Exception:
                obj = {}
            return resp.status_code, obj, txt
        except Exception as exc:
            return -1, {}, f"request_error: {type(exc).__name__}: {exc}"

    def _post_chat(self, payload: Dict[str, Any]) -> Tuple[int, Dict[str, Any], str]:
        return self._post_json("/chat/completions", payload)

    def _post_responses(self, payload: Dict[str, Any]) -> Tuple[int, Dict[str, Any], str]:
        return self._post_json("/responses", payload)

    @staticmethod
    def _extract_chat_text(obj: Dict[str, Any]) -> str:
        msg = obj.get("choices", [{}])[0].get("message", {}).get("content", "")
        if isinstance(msg, str):
            return msg
        if isinstance(msg, list):
            parts = []
            for item in msg:
                if isinstance(item, dict) and item.get("type") == "text":
                    parts.append(str(item.get("text", "")))
            return "\n".join([p for p in parts if p])
        return ""

    @staticmethod
    def _extract_responses_text(obj: Dict[str, Any]) -> str:
        direct = obj.get("output_text")
        if isinstance(direct, str) and direct.strip():
            return direct
        parts: List[str] = []
        for item in obj.get("output", []):
            if not isinstance(item, dict):
                continue
            for content in item.get("content", []):
                if not isinstance(content, dict):
                    continue
                text = content.get("text")
                if isinstance(text, str) and text.strip():
                    parts.append(text)
        return "\n".join(parts).strip()

    def _try_responses(self, model_id: str, prompt: str, b64: str, max_output_tokens: int) -> Tuple[bool, Dict[str, Any]]:
        payload = {
            "model": model_id,
            "input": [
                {
                    "role": "user",
                    "content": [
                        {"type": "input_text", "text": prompt},
                        {"type": "input_image", "image_url": f"data:image/jpeg;base64,{b64}"},
                    ],
                }
            ],
            "max_output_tokens": max_output_tokens,
        }
        status, obj, raw = self._post_responses(payload)
        if status == 200 and obj:
            msg = self._extract_responses_text(obj)
            parsed = parse_json_object(msg)
            if parsed:
                return True, {
                    "ok": True,
                    "status": status,
                    "raw": msg,
                    "json": parsed,
                    "usage": obj.get("usage", {}),
                    "resolved_model": model_id,
                    "resolved_endpoint": "responses",
                }
            return False, {"error": f"{model_id}: responses non-json", "raw": raw[:240], "status": status}
        return False, {"error": f"{model_id}: responses status={status} raw={raw[:240]}", "raw": raw[:240], "status": status}

    def _try_chat(self, model_id: str, prompt: str, b64: str, max_output_tokens: int) -> Tuple[bool, Dict[str, Any]]:
        headers = {
            "Authorization": f"Bearer {self.profile.api_key}",
            "Content-Type": "application/json",
        }
        _ = headers
        payload_openai = {
            "model": model_id,
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": prompt},
                        {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{b64}"}},
                    ],
                }
            ],
            "temperature": 0,
            "max_tokens": max_output_tokens,
        }
        status, obj, raw = self._post_chat(payload_openai)
        if status == 200 and obj:
            msg = self._extract_chat_text(obj)
            parsed = parse_json_object(msg)
            if parsed:
                return True, {
                    "ok": True,
                    "status": status,
                    "raw": msg,
                    "json": parsed,
                    "usage": obj.get("usage", {}),
                    "resolved_model": model_id,
                    "resolved_endpoint": "chat_completions",
                }
        payload_alt = {
            "model": model_id,
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": prompt},
                        {
                            "type": "image",
                            "source": {
                                "type": "base64",
                                "media_type": "image/jpeg",
                                "data": b64,
                            },
                        },
                    ],
                }
            ],
            "temperature": 0,
            "max_tokens": max_output_tokens,
        }
        status2, obj2, raw2 = self._post_chat(payload_alt)
        if status2 == 200 and obj2:
            msg2 = self._extract_chat_text(obj2)
            parsed2 = parse_json_object(msg2)
            if parsed2:
                return True, {
                    "ok": True,
                    "status": status2,
                    "raw": msg2,
                    "json": parsed2,
                    "usage": obj2.get("usage", {}),
                    "resolved_model": model_id,
                    "resolved_endpoint": "chat_completions_alt",
                }
        err = f"{model_id}: chat status={status} raw={raw[:160]} || alt status={status2} raw={raw2[:160]}"
        return False, {"error": err, "status": status2 if status2 != 200 else status, "raw": raw2[:240] or raw[:240]}

    def classify_page(
        self,
        image_jpeg: bytes,
        file_name: str,
        page_no: int,
        custom_prompt: str,
        memory_notes: List[str],
        page_cues: Optional[Dict[str, Any]] = None,
        highres_right_strip_jpeg: Optional[bytes] = None,
    ) -> Dict[str, Any]:
        review_jpeg = build_llm_review_jpeg(image_jpeg, highres_right_strip_jpeg=highres_right_strip_jpeg)
        b64 = base64.b64encode(review_jpeg).decode("ascii")
        rules = ""
        if memory_notes:
            joined = "\n".join([f"- {x}" for x in memory_notes[:20]])
            rules = f"\nLearned rules from prior corrections:\n{joined}\n"
        cues = ""
        if page_cues:
            cue_lines = [f"- rendered_size={page_cues.get('image_width')}x{page_cues.get('image_height')}"]
            if page_cues.get("content_width") and page_cues.get("content_height"):
                cue_lines.append(
                    f"- content_bbox_size={page_cues.get('content_width')}x{page_cues.get('content_height')} "
                    f"(ratio={page_cues.get('content_ratio')})"
                )
                cue_lines.append(
                    f"- clean_ratio_band={page_cues.get('clean_ratio_low')}..{page_cues.get('clean_ratio_high')} "
                    f"baseline={page_cues.get('clean_ratio_baseline')} "
                    f"content_ratio_in_clean_band={page_cues.get('content_ratio_in_clean_band')} "
                    f"content_ratio_error_pct={page_cues.get('content_ratio_error_pct')}"
                )
            if page_cues.get("ratio_guided_boundary_x") is not None:
                cue_lines.append(
                    f"- ratio_guided_boundary_x={page_cues.get('ratio_guided_boundary_x')} "
                    f"method={page_cues.get('boundary_method')} "
                    f"reliable={page_cues.get('ratio_guided_boundary_reliable')} "
                    f"left_ratio={page_cues.get('ratio_guided_left_ratio')} "
                    f"error_pct={page_cues.get('ratio_guided_boundary_error_pct')} "
                    f"strength={page_cues.get('right_boundary_strength')}"
                )
                cue_lines.append(
                    f"- right_of_boundary_dark_ratio={page_cues.get('right_of_boundary_dark_ratio')} "
                    f"right_of_boundary_colmax_ratio={page_cues.get('right_of_boundary_colmax_ratio')} "
                    f"outside_structured_content={page_cues.get('outside_structured_content')}"
                )
            cue_lines.append(
                "- review_image_layout=top panel is the full page; bottom-left is a zoomed right-edge crop; bottom-middle is a high-contrast zoomed right-edge crop; bottom-right is a high-contrast extreme-right micro-strip"
            )
            cue_lines.append(
                f"- right_strip_6_dark_ratio={page_cues.get('right_strip_dark_ratio_6')} "
                f"right_strip_6_peak_cols={page_cues.get('right_strip_peak_cols_6')} "
                f"right_strip_6_maxcol_ratio={page_cues.get('right_strip_maxcol_ratio_6')}"
            )
            cue_lines.append(
                f"- right_strip_8_dark_ratio={page_cues.get('right_strip_dark_ratio_8')} "
                f"right_strip_8_peak_cols={page_cues.get('right_strip_peak_cols_8')} "
                f"right_strip_8_maxcol_ratio={page_cues.get('right_strip_maxcol_ratio_8')}"
            )
            cue_lines.append(
                f"- right_strip_10_dark_ratio={page_cues.get('right_strip_dark_ratio_10')} "
                f"right_strip_10_peak_cols={page_cues.get('right_strip_peak_cols_10')} "
                f"right_strip_10_maxcol_ratio={page_cues.get('right_strip_maxcol_ratio_10')}"
            )
            cue_lines.append(
                f"- subtle_right_strip_suspect={page_cues.get('subtle_right_strip_suspect')}"
            )
            cue_lines.append(
                f"- correct_black_edge_trimmed_page_body_ratio={page_cues.get('trimmed_body_ratio')} "
                f"trimmed_body_band_match={page_cues.get('trimmed_body_in_clean_band')} "
                f"trimmed_body_wide_hint={page_cues.get('trimmed_body_wide_hint')} "
                f"trimmed_body_top_spread={page_cues.get('trimmed_body_top_spread')} "
                f"trimmed_body_right_minus_left={page_cues.get('trimmed_body_right_minus_left')} "
                f"trimmed_body_irregular_overlap={page_cues.get('trimmed_body_irregular_overlap')}"
            )
            cue_lines.append(
                f"- right_strip_source={page_cues.get('right_strip_source')}"
            )
            cue_lines.append(
                "- heuristic_rule=do not use an internal grade-column divider or table separator as the first-page boundary"
            )
            cue_lines.append(
                "- heuristic_rule=if content_ratio_in_clean_band=true and boundary_method=bbox_right_edge, there is no boundary-overflow evidence; only direct ghost/superimposed second-card evidence can justify overlap"
            )
            cue_lines.append(
                "- heuristic_rule=only treat content to the right as overlap evidence when ratio_guided_boundary_reliable=true"
            )
            cue_lines.append(
                "- heuristic_rule=if subtle_right_strip_suspect=true, inspect the bottom panels carefully for a narrow extra structured strip on the extreme right; that can still be a ghost overlap even when the full page ratio looks normal"
            )
            cue_lines.append(
                "- heuristic_rule=the bottom-right micro-strip is the strongest view for subtle edge overlaps; a second dark border or second structured vertical band hugging the far right edge is strong overlap evidence"
            )
            cue_lines.append(
                "- heuristic_rule=correct black-edge-trimmed page-body ratio is only a weighting cue; use it to raise suspicion or confidence, not as a standalone overlap proof"
            )
            cue_lines.append(
                "- heuristic_rule=if trimmed_body_irregular_overlap=true, the correct black-edge-trimmed page-body top boundary is strongly distorted on the right side; that is direct overlap evidence"
            )
            cues = "\nMeasured visual cues:\n" + "\n".join(cue_lines) + "\n"
        prompt = (
            DEFAULT_CLASSIFY_PROMPT
            + rules
            + cues
            + f"\nfile={file_name}\npage={page_no}\n"
        )
        if custom_prompt.strip():
            prompt += f"\nCustom instructions:\n{custom_prompt.strip()}\n"

        errors: List[str] = []
        for model_id in self.model_candidates:
            ok, result = self._try_responses(model_id, prompt, b64, 320)
            if ok:
                return result
            errors.append(str(result.get("error", ""))[:220])
            if self.responses_only:
                continue

            ok, result = self._try_chat(model_id, prompt, b64, 320)
            if ok:
                return result
            errors.append(str(result.get("error", ""))[:220])

        return {
            "ok": False,
            "status": -1,
            "raw": "",
            "error": "Image classification failed. " + " || ".join(errors[:4]),
        }

    def quick_test(self) -> Tuple[bool, str]:
        tiny_img = Image.new("RGB", (12, 12), color="white")
        bio = io.BytesIO()
        tiny_img.save(bio, format="JPEG", quality=70)
        b64 = base64.b64encode(bio.getvalue()).decode("ascii")
        errs = []
        for model_id in self.model_candidates:
            payload = {
                "model": model_id,
                "input": [
                    {
                        "role": "user",
                        "content": [
                            {"type": "input_text", "text": "Reply exactly OK."},
                            {"type": "input_image", "image_url": f"data:image/jpeg;base64,{b64}"},
                        ],
                    }
                ],
                "max_output_tokens": 12,
            }
            status, obj, raw = self._post_responses(payload)
            if status == 200 and obj:
                msg = self._extract_responses_text(obj)
                return True, f"HTTP 200 via '{model_id}' on responses. Reply: {msg[:120]!r}"
            errs.append(f"{model_id}: responses {status} {raw[:80]}")
            if self.responses_only:
                continue

            ok, result = self._try_chat(model_id, "Reply exactly OK.", b64, 12)
            if ok:
                return True, f"HTTP 200 via '{model_id}' on {result.get('resolved_endpoint')}. Reply: {str(result.get('raw', ''))[:120]!r}"
            errs.append(f"{model_id}: chat fallback failed")
        return False, " ; ".join(errs[:3])


class OverlapEngine:
    def __init__(
        self,
        client: OpenAICompatibleClient,
        verifier_client: Optional[OpenAICompatibleClient],
        memory: Dict[str, Any],
        logger,
        cancel_event: threading.Event,
        pause_event: Optional[threading.Event],
        progress_cb,
        render_dpi: int = 220,
    ) -> None:
        self.client = client
        self.verifier_client = verifier_client
        self.memory = ensure_memory_schema(memory)
        self.log = logger
        self.cancel_event = cancel_event
        self.pause_event = pause_event
        self.progress_cb = progress_cb
        self.render_dpi = max(120, min(360, int(render_dpi)))
        self.batch_ratio_info: Dict[str, Any] = {}

    @staticmethod
    def _blank_verifier_fields() -> Dict[str, Any]:
        return {
            "verifier_model": "",
            "verifier_decision": "",
            "verifier_confidence": "",
            "verifier_reason": "",
        }

    @staticmethod
    def _float_cue(page_cues: Dict[str, Any], key: str) -> float:
        try:
            return float(page_cues.get(key, 0.0) or 0.0)
        except Exception:
            return 0.0

    @staticmethod
    def _int_cue(page_cues: Dict[str, Any], key: str) -> int:
        try:
            return int(page_cues.get(key, 0) or 0)
        except Exception:
            return 0

    def _should_run_verifier(
        self,
        decision: str,
        confidence: float,
        page_cues: Dict[str, Any],
    ) -> bool:
        if not self.verifier_client:
            return False
        if decision == "overlap":
            return False
        if decision == "uncertain":
            return True
        if decision == "blurry":
            return False

        subtle = bool(page_cues.get("subtle_right_strip_suspect"))
        in_band = bool(page_cues.get("content_ratio_in_clean_band"))
        boundary_reliable = bool(page_cues.get("ratio_guided_boundary_reliable"))
        boundary_method = str(page_cues.get("boundary_method", ""))
        right_dark = self._float_cue(page_cues, "right_of_boundary_dark_ratio")
        right_colmax = self._float_cue(page_cues, "right_of_boundary_colmax_ratio")
        strip_dark_8 = self._float_cue(page_cues, "right_strip_dark_ratio_8")
        strip_peak_8 = self._int_cue(page_cues, "right_strip_peak_cols_8")
        trimmed_body_wide_hint = bool(page_cues.get("trimmed_body_wide_hint"))
        trimmed_body_irregular_overlap = bool(page_cues.get("trimmed_body_irregular_overlap"))

        if subtle:
            return True
        if trimmed_body_irregular_overlap or trimmed_body_wide_hint:
            return True
        if (
            not in_band
            and boundary_reliable
            and boundary_method.startswith("ratio_guided")
            and (right_dark >= 0.08 or right_colmax >= 0.35)
        ):
            return True
        if confidence < 0.88 and (strip_dark_8 >= 0.68 or strip_peak_8 >= 160):
            return True
        return False

    def _run_verifier(
        self,
        image_jpeg: bytes,
        file_name: str,
        page_no: int,
        custom_prompt: str,
        page_cues: Dict[str, Any],
        highres_right_strip_jpeg: Optional[bytes],
    ) -> Dict[str, Any]:
        verifier_fields = self._blank_verifier_fields()
        if not self.verifier_client:
            return verifier_fields

        verify_result = self.verifier_client.classify_page(
            image_jpeg=image_jpeg,
            file_name=file_name,
            page_no=page_no,
            custom_prompt=custom_prompt,
            memory_notes=build_memory_notes(self.memory, file_name),
            page_cues=page_cues,
            highres_right_strip_jpeg=highres_right_strip_jpeg,
        )
        if not verify_result.get("ok"):
            verifier_fields["verifier_model"] = self.verifier_client.profile.name
            verifier_fields["verifier_reason"] = (
                "verifier_error: "
                + f"status={verify_result.get('status')} "
                + str(verify_result.get("error", "unknown"))[:220]
            )
            if bool(page_cues.get("subtle_right_strip_suspect")):
                microstrip_fields = self._run_microstrip_verifier(
                    image_jpeg=image_jpeg,
                    highres_right_strip_jpeg=highres_right_strip_jpeg,
                )
                if microstrip_fields.get("verifier_decision"):
                    verifier_fields.update(microstrip_fields)
            return verifier_fields

        vobj = verify_result.get("json", {})
        vdecision, vis_overlap, vis_blurry = normalize_decision_fields(vobj)
        vdecision, vis_overlap, vis_blurry, vguard_reason = apply_ratio_boundary_guard(
            decision=vdecision,
            is_overlap=vis_overlap,
            is_blurry=vis_blurry,
            overlap_type=str(vobj.get("overlap_type", "none")),
            confidence=float(vobj.get("confidence", 0.0)),
            page_cues=page_cues,
        )
        vreason = str(vobj.get("reason", ""))[:320]
        if vguard_reason:
            vreason = (vreason + " | " if vreason else "") + vguard_reason
        verifier_fields.update(
            {
                "verifier_model": self.verifier_client.profile.name,
                "verifier_decision": vdecision,
                "verifier_confidence": float(vobj.get("confidence", 0.0)),
                "verifier_reason": vreason,
                "_verifier_is_overlap": vis_overlap,
                "_verifier_is_blurry": vis_blurry,
                "_verifier_overlap_type": str(vobj.get("overlap_type", "none")),
                "_verifier_resolved_model": verify_result.get("resolved_model", ""),
                "_verifier_resolved_endpoint": verify_result.get("resolved_endpoint", ""),
            }
        )
        if bool(page_cues.get("subtle_right_strip_suspect")):
            microstrip_fields = self._run_microstrip_verifier(
                image_jpeg=image_jpeg,
                highres_right_strip_jpeg=highres_right_strip_jpeg,
            )
            if (
                microstrip_fields.get("verifier_decision") == "overlap"
                and float(microstrip_fields.get("verifier_confidence", 0.0) or 0.0) >= 0.80
            ):
                verifier_fields.update(microstrip_fields)
        return verifier_fields

    def _run_microstrip_verifier(
        self,
        image_jpeg: bytes,
        highres_right_strip_jpeg: Optional[bytes],
    ) -> Dict[str, Any]:
        verifier_fields = self._blank_verifier_fields()
        if not self.verifier_client:
            return verifier_fields
        prompt = (
            "Classify this image, which is only the extreme-right micro-strip from a microfiche transcript page. "
            "Return strict JSON only with keys decision, confidence, and reason. "
            "decision must be one of [overlap_edge, normal_edge, uncertain]. "
            "Use overlap_edge only if you see a second border/frame or extra structured vertical band hugging the far right edge beyond the main card. "
            "A single normal outer border or slanted corner without an extra structured strip is normal_edge."
        )
        microstrip_jpeg = build_extreme_right_microstrip_jpeg(
            image_jpeg=image_jpeg,
            highres_right_strip_jpeg=highres_right_strip_jpeg,
        )
        b64 = base64.b64encode(microstrip_jpeg).decode("ascii")

        last_error = ""
        for model_id in self.verifier_client.model_candidates:
            ok, result = self.verifier_client._try_chat(model_id, prompt, b64, 120)
            if ok:
                obj = result.get("json", {})
                raw_decision = str(obj.get("decision", "")).strip().lower()
                try:
                    confidence = float(obj.get("confidence", 0.0) or 0.0)
                except Exception:
                    confidence = 0.0
                if raw_decision == "overlap_edge":
                    decision = "overlap"
                    if confidence <= 0.0:
                        confidence = 0.85
                elif raw_decision == "normal_edge":
                    decision = "clean"
                    if confidence <= 0.0:
                        confidence = 0.80
                else:
                    decision = "uncertain"
                verifier_fields.update(
                    {
                        "verifier_model": f"{self.verifier_client.profile.name} microstrip",
                        "verifier_decision": decision,
                        "verifier_confidence": confidence,
                        "verifier_reason": ("microstrip_verifier: " + str(obj.get("reason", ""))[:260]).strip(),
                        "_verifier_is_overlap": decision == "overlap",
                        "_verifier_is_blurry": False,
                        "_verifier_overlap_type": "ghost_superimposition" if decision == "overlap" else "none",
                        "_verifier_resolved_model": result.get("resolved_model", ""),
                        "_verifier_resolved_endpoint": result.get("resolved_endpoint", ""),
                    }
                )
                return verifier_fields
            last_error = str(result.get("error", ""))[:220]

        verifier_fields["verifier_model"] = f"{self.verifier_client.profile.name} microstrip"
        verifier_fields["verifier_reason"] = f"microstrip_verifier_error: {last_error}"
        return verifier_fields

    def memory_override(self, file_name: str, page_no: int) -> Optional[Dict[str, Any]]:
        key = f"{file_name.lower()}::{page_no}"
        return self.memory.get("overrides", {}).get(key)

    def _wait_if_paused(self) -> None:
        while self.pause_event and self.pause_event.is_set() and not self.cancel_event.is_set():
            time.sleep(0.15)

    def scan_pdfs(
        self,
        pdf_paths: List[Path],
        scope: str,
        custom_prompt: str,
        on_page_result: Optional[Callable[[Dict[str, Any], Path, fitz.Document], None]] = None,
        on_file_done: Optional[Callable[[Path, fitz.Document, List[Dict[str, Any]]], None]] = None,
    ) -> List[Dict[str, Any]]:
        records: List[Dict[str, Any]] = []
        total_pages = 0
        for p in pdf_paths:
            try:
                doc = fitz.open(str(p))
                total_pages += len(doc)
                doc.close()
            except Exception:
                pass
        done = 0
        self.progress_cb(done, max(total_pages, 1))

        if pdf_paths and (scope == "source" or not self.batch_ratio_info):
            try:
                self.log(f"Estimating clean single-card geometry baseline for {scope} pages...")
                self.batch_ratio_info = estimate_clean_ratio_baseline(
                    pdf_paths,
                    dpi=self.render_dpi,
                    max_width=100000,
                    quality=55,
                )
                self.log(
                    "Geometry baseline: "
                    f"baseline={self.batch_ratio_info.get('baseline_ratio')} "
                    f"band={self.batch_ratio_info.get('ratio_low')}..{self.batch_ratio_info.get('ratio_high')} "
                    f"body_width={self.batch_ratio_info.get('baseline_body_width')} "
                    f"width_rel_overlap>{self.batch_ratio_info.get('body_width_overlap_rel_threshold')} "
                    f"pages={self.batch_ratio_info.get('page_count')}"
                )
            except Exception as exc:
                self.batch_ratio_info = {
                    "baseline_ratio": 2.24,
                    "ratio_tolerance": 0.05,
                    "ratio_low": round(2.24 * 0.95, 3),
                    "ratio_high": round(2.24 * 1.05, 3),
                    "baseline_body_width": 0.0,
                    "body_width_overlap_rel_threshold": PY_WIDTH_OVERLAP_REL_THRESHOLD,
                    "body_width_overlap_threshold": 0.0,
                }
                self.log(f"Geometry baseline estimation failed, using default ratio band only: {exc}")
        elif self.batch_ratio_info:
            self.log(
                "Reusing geometry baseline: "
                f"baseline={self.batch_ratio_info.get('baseline_ratio')} "
                f"band={self.batch_ratio_info.get('ratio_low')}..{self.batch_ratio_info.get('ratio_high')} "
                f"body_width={self.batch_ratio_info.get('baseline_body_width')}"
            )

        for pdf_path in pdf_paths:
            self._wait_if_paused()
            if self.cancel_event.is_set():
                self.log("Scan cancelled.")
                break

            self.log(f"Scanning: {pdf_path}")
            try:
                doc = fitz.open(str(pdf_path))
            except Exception as exc:
                self.log(f"Failed to open {pdf_path}: {exc}")
                continue
            file_records: List[Dict[str, Any]] = []

            for idx in range(len(doc)):
                self._wait_if_paused()
                if self.cancel_event.is_set():
                    self.log("Scan cancelled.")
                    break
                page_no = idx + 1
                file_name = pdf_path.name
                override = self.memory_override(file_name, page_no)
                if override:
                    override_decision = str(override.get("decision", "")).strip().lower()
                    if override_decision not in {"overlap", "blurry", "clean", "uncertain"}:
                        override_overlap = bool(override.get("is_overlap", False))
                        override_blurry = bool(override.get("is_blurry", False))
                        override_decision = "overlap" if override_overlap else ("blurry" if override_blurry else "clean")
                    override_overlap = bool(override.get("is_overlap", override_decision == "overlap"))
                    override_blurry = bool(override.get("is_blurry", override_decision == "blurry"))
                    rec = {
                        "source_directory": str(pdf_path.parent),
                        "file_name": file_name,
                        "file_path": str(pdf_path),
                        "page": page_no,
                        "decision": override_decision,
                        "is_overlap": override_overlap,
                        "is_blurry": override_blurry,
                        "confidence": float(override.get("confidence", 1.0)),
                        "overlap_type": str(override.get("overlap_type", "manual_override")),
                        "signatures": override.get("signatures", []),
                        "reason": str(override.get("note", "manual memory override")),
                        "scope": scope,
                        "model": self.client.profile.model,
                        "resolved_model": "memory_override",
                        "endpoint": "memory_override",
                        "status": "memory_override",
                        "error_detail": "",
                        **self._blank_verifier_fields(),
                        "content_ratio": "",
                        "clean_ratio_low": "",
                        "clean_ratio_high": "",
                        "content_ratio_in_clean_band": "",
                        "boundary_method": "",
                        "boundary_reliable": "",
                        "ratio_guided_left_ratio": "",
                        "right_boundary_x": "",
                        "right_of_boundary_dark_ratio": "",
                        "right_of_boundary_colmax_ratio": "",
                        "right_strip_dark_ratio_6": "",
                        "right_strip_peak_cols_6": "",
                        "right_strip_dark_ratio_8": "",
                        "right_strip_peak_cols_8": "",
                        "right_strip_dark_ratio_10": "",
                        "right_strip_peak_cols_10": "",
                        "subtle_right_strip_suspect": "",
                    }
                    records.append(rec)
                    file_records.append(rec)
                    self.log("Page result (memory): " + summarize_page_result(rec))
                    if on_page_result:
                        try:
                            on_page_result(rec, pdf_path, doc)
                        except Exception as cb_exc:
                            self.log(f"on_page_result callback failed: {cb_exc}")
                    done += 1
                    self.progress_cb(done, max(total_pages, 1))
                    continue

                try:
                    image_jpeg = render_page_jpeg(doc[idx], dpi=self.render_dpi, max_width=100000)
                    page_cues = measure_page_visual_cues(
                        image_jpeg,
                        ratio_baseline=float(self.batch_ratio_info.get("baseline_ratio") or 2.24),
                        ratio_tolerance=float(self.batch_ratio_info.get("ratio_tolerance") or 0.05),
                    )
                    highres_right_strip_jpeg = None
                    if page_cues.get("content_ratio_in_clean_band"):
                        try:
                            highres_right_strip_jpeg, highres_strip_cues = render_highres_right_strip_jpeg(doc[idx], dpi=360)
                            page_cues.update(highres_strip_cues)
                            page_cues["subtle_right_strip_suspect"] = subtle_right_strip_flag(
                                page_cues, bool(page_cues.get("content_ratio_in_clean_band"))
                            )
                            page_cues["right_strip_source"] = "highres"
                        except Exception as strip_exc:
                            page_cues["right_strip_source"] = f"lowres_fallback:{type(strip_exc).__name__}"
                    else:
                        page_cues["right_strip_source"] = "lowres"
                except Exception as exc:
                    self.log(f"Render failed {pdf_path} p{page_no}: {exc}")
                    rec = {
                        "source_directory": str(pdf_path.parent),
                        "file_name": file_name,
                        "file_path": str(pdf_path),
                        "page": page_no,
                        "decision": "uncertain",
                        "is_overlap": False,
                        "is_blurry": False,
                        "confidence": 0.0,
                        "overlap_type": "none",
                        "signatures": [],
                        "reason": f"render_error: {exc}",
                        "scope": scope,
                        "model": self.client.profile.model,
                        "resolved_model": "",
                        "endpoint": "",
                        "status": "error",
                        "error_detail": f"render_error: {exc}",
                        **self._blank_verifier_fields(),
                        "content_ratio": "",
                        "clean_ratio_low": "",
                        "clean_ratio_high": "",
                        "content_ratio_in_clean_band": "",
                        "boundary_method": "",
                        "boundary_reliable": "",
                        "ratio_guided_left_ratio": "",
                        "right_boundary_x": "",
                        "right_of_boundary_dark_ratio": "",
                        "right_of_boundary_colmax_ratio": "",
                        "right_strip_dark_ratio_6": "",
                        "right_strip_peak_cols_6": "",
                        "right_strip_dark_ratio_8": "",
                        "right_strip_peak_cols_8": "",
                        "right_strip_dark_ratio_10": "",
                        "right_strip_peak_cols_10": "",
                        "subtle_right_strip_suspect": "",
                    }
                    records.append(rec)
                    file_records.append(rec)
                    self.log("Page result (error): " + summarize_page_result(rec))
                    if on_page_result:
                        try:
                            on_page_result(rec, pdf_path, doc)
                        except Exception as cb_exc:
                            self.log(f"on_page_result callback failed: {cb_exc}")
                    done += 1
                    self.progress_cb(done, max(total_pages, 1))
                    continue

                result = self.client.classify_page(
                    image_jpeg=image_jpeg,
                    file_name=file_name,
                    page_no=page_no,
                    custom_prompt=custom_prompt,
                    memory_notes=build_memory_notes(self.memory, file_name),
                    page_cues=page_cues,
                    highres_right_strip_jpeg=highres_right_strip_jpeg,
                )

                if not result.get("ok"):
                    verifier_fields = self._blank_verifier_fields()
                    verifier_result = self._run_verifier(
                        image_jpeg=image_jpeg,
                        file_name=file_name,
                        page_no=page_no,
                        custom_prompt=custom_prompt,
                        page_cues=page_cues,
                        highres_right_strip_jpeg=highres_right_strip_jpeg,
                    )
                    if verifier_result.get("verifier_decision"):
                        verifier_fields.update(
                            {k: verifier_result.get(k, "") for k in self._blank_verifier_fields().keys()}
                        )
                        rec = {
                            "source_directory": str(pdf_path.parent),
                            "file_name": file_name,
                            "file_path": str(pdf_path),
                            "page": page_no,
                            "decision": verifier_result.get("verifier_decision", "uncertain"),
                            "is_overlap": bool(verifier_result.get("_verifier_is_overlap", False)),
                            "is_blurry": bool(verifier_result.get("_verifier_is_blurry", False)),
                            "confidence": float(verifier_result.get("verifier_confidence", 0.0) or 0.0),
                            "overlap_type": str(verifier_result.get("_verifier_overlap_type", "none")),
                            "signatures": [],
                            "reason": (
                                "primary_llm_error -> verifier takeover"
                                + (
                                    f" | {verifier_result.get('verifier_reason')}"
                                    if verifier_result.get("verifier_reason")
                                    else ""
                                )
                            )[:500],
                            "scope": scope,
                            "model": self.client.profile.model,
                            "resolved_model": verifier_result.get("_verifier_resolved_model", ""),
                            "endpoint": verifier_result.get("_verifier_resolved_endpoint", ""),
                            "status": "verifier_fallback",
                            "error_detail": f"primary={result.get('error', 'unknown')}"[:240],
                            **verifier_fields,
                            "content_ratio": page_cues.get("content_ratio", ""),
                            "clean_ratio_low": page_cues.get("clean_ratio_low", ""),
                            "clean_ratio_high": page_cues.get("clean_ratio_high", ""),
                            "content_ratio_in_clean_band": page_cues.get("content_ratio_in_clean_band", ""),
                            "boundary_method": page_cues.get("boundary_method", ""),
                            "boundary_reliable": page_cues.get("ratio_guided_boundary_reliable", ""),
                            "ratio_guided_left_ratio": page_cues.get("ratio_guided_left_ratio", ""),
                            "right_boundary_x": page_cues.get("ratio_guided_boundary_x", ""),
                            "right_of_boundary_dark_ratio": page_cues.get("right_of_boundary_dark_ratio", ""),
                            "right_of_boundary_colmax_ratio": page_cues.get("right_of_boundary_colmax_ratio", ""),
                            "right_strip_dark_ratio_6": page_cues.get("right_strip_dark_ratio_6", ""),
                            "right_strip_peak_cols_6": page_cues.get("right_strip_peak_cols_6", ""),
                            "right_strip_dark_ratio_8": page_cues.get("right_strip_dark_ratio_8", ""),
                            "right_strip_peak_cols_8": page_cues.get("right_strip_peak_cols_8", ""),
                            "right_strip_dark_ratio_10": page_cues.get("right_strip_dark_ratio_10", ""),
                            "right_strip_peak_cols_10": page_cues.get("right_strip_peak_cols_10", ""),
                            "subtle_right_strip_suspect": page_cues.get("subtle_right_strip_suspect", ""),
                        }
                        self.log(
                            f"Primary model failed {file_name} p{page_no}; "
                            f"verifier {verifier_fields.get('verifier_model')} produced {rec['decision']} "
                            f"(conf={float(rec.get('confidence', 0.0)):.2f})."
                        )
                    else:
                        rec = {
                            "source_directory": str(pdf_path.parent),
                            "file_name": file_name,
                            "file_path": str(pdf_path),
                            "page": page_no,
                            "decision": "uncertain",
                            "is_overlap": False,
                            "is_blurry": False,
                            "confidence": 0.0,
                            "overlap_type": "none",
                            "signatures": [],
                            "reason": "llm_error",
                            "scope": scope,
                            "model": self.client.profile.model,
                            "resolved_model": result.get("resolved_model", ""),
                            "endpoint": result.get("resolved_endpoint", ""),
                            "status": "llm_error",
                            "error_detail": f"status={result.get('status')} err={result.get('error', 'unknown')} raw={str(result.get('raw', ''))[:240]}",
                            **verifier_fields,
                            "content_ratio": page_cues.get("content_ratio", ""),
                            "clean_ratio_low": page_cues.get("clean_ratio_low", ""),
                            "clean_ratio_high": page_cues.get("clean_ratio_high", ""),
                            "content_ratio_in_clean_band": page_cues.get("content_ratio_in_clean_band", ""),
                            "boundary_method": page_cues.get("boundary_method", ""),
                            "boundary_reliable": page_cues.get("ratio_guided_boundary_reliable", ""),
                            "ratio_guided_left_ratio": page_cues.get("ratio_guided_left_ratio", ""),
                            "right_boundary_x": page_cues.get("ratio_guided_boundary_x", ""),
                            "right_of_boundary_dark_ratio": page_cues.get("right_of_boundary_dark_ratio", ""),
                            "right_of_boundary_colmax_ratio": page_cues.get("right_of_boundary_colmax_ratio", ""),
                            "right_strip_dark_ratio_6": page_cues.get("right_strip_dark_ratio_6", ""),
                            "right_strip_peak_cols_6": page_cues.get("right_strip_peak_cols_6", ""),
                            "right_strip_dark_ratio_8": page_cues.get("right_strip_dark_ratio_8", ""),
                            "right_strip_peak_cols_8": page_cues.get("right_strip_peak_cols_8", ""),
                            "right_strip_dark_ratio_10": page_cues.get("right_strip_dark_ratio_10", ""),
                            "right_strip_peak_cols_10": page_cues.get("right_strip_peak_cols_10", ""),
                            "subtle_right_strip_suspect": page_cues.get("subtle_right_strip_suspect", ""),
                        }
                    self.log(
                        f"LLM failed {file_name} p{page_no}: "
                        f"{rec['error_detail']}"
                    )
                else:
                    obj = result.get("json", {})
                    sigs = obj.get("signatures", [])
                    if not isinstance(sigs, list):
                        sigs = []
                    sigs = [norm_sig(str(s)) for s in sigs if str(s).strip()][:2]
                    decision, is_overlap, is_blurry = normalize_decision_fields(obj)
                    guard_reason = None
                    decision, is_overlap, is_blurry, guard_reason = apply_ratio_boundary_guard(
                        decision=decision,
                        is_overlap=is_overlap,
                        is_blurry=is_blurry,
                        overlap_type=str(obj.get("overlap_type", "none")),
                        confidence=float(obj.get("confidence", 0.0)),
                        page_cues=page_cues,
                    )
                    reason_text = str(obj.get("reason", ""))[:500]
                    if guard_reason:
                        if reason_text:
                            reason_text = f"{reason_text} | {guard_reason}"
                        else:
                            reason_text = guard_reason
                    verifier_fields = self._blank_verifier_fields()
                    primary_confidence = float(obj.get("confidence", 0.0))
                    if self._should_run_verifier(decision, primary_confidence, page_cues):
                        verifier_result = self._run_verifier(
                            image_jpeg=image_jpeg,
                            file_name=file_name,
                            page_no=page_no,
                            custom_prompt=custom_prompt,
                            page_cues=page_cues,
                            highres_right_strip_jpeg=highres_right_strip_jpeg,
                        )
                        verifier_fields.update(
                            {k: verifier_result.get(k, "") for k in self._blank_verifier_fields().keys()}
                        )
                        if verifier_fields.get("verifier_decision"):
                            self.log(
                                f"Verifier review {file_name} p{page_no}: "
                                f"primary={decision}/{primary_confidence:.2f} -> "
                                f"{verifier_fields.get('verifier_model')}="
                                f"{verifier_fields.get('verifier_decision')}/"
                                f"{float(verifier_fields.get('verifier_confidence', 0.0) or 0.0):.2f}"
                            )
                        if (
                            verifier_result.get("verifier_decision") == "overlap"
                            and bool(verifier_result.get("_verifier_is_overlap", False))
                            and float(verifier_result.get("verifier_confidence", 0.0) or 0.0) >= 0.80
                        ):
                            decision = "overlap"
                            is_overlap = True
                            is_blurry = False
                            primary_overlap_type = str(obj.get("overlap_type", "none"))
                            verifier_overlap_type = str(verifier_result.get("_verifier_overlap_type", "none"))
                            if primary_overlap_type == "none" and verifier_overlap_type:
                                obj["overlap_type"] = verifier_overlap_type
                            obj["confidence"] = max(
                                primary_confidence,
                                float(verifier_result.get("verifier_confidence", 0.0) or 0.0),
                            )
                            promotion_reason = (
                                f"verifier({verifier_fields.get('verifier_model')}) promoted "
                                f"{str(obj.get('decision', 'clean')).strip().lower()} -> overlap: "
                                f"{verifier_fields.get('verifier_reason', '')}"
                            ).strip()
                            reason_text = (reason_text + " | " if reason_text else "") + promotion_reason[:320]
                    rec = {
                        "source_directory": str(pdf_path.parent),
                        "file_name": file_name,
                        "file_path": str(pdf_path),
                        "page": page_no,
                        "decision": decision,
                        "is_overlap": is_overlap,
                        "is_blurry": is_blurry,
                        "confidence": float(obj.get("confidence", 0.0)),
                        "overlap_type": str(obj.get("overlap_type", "none")),
                        "signatures": sigs,
                        "reason": reason_text,
                        "scope": scope,
                        "model": self.client.profile.model,
                        "resolved_model": result.get("resolved_model", ""),
                        "endpoint": result.get("resolved_endpoint", ""),
                        "status": "ok",
                        "error_detail": "",
                        **verifier_fields,
                        "content_ratio": page_cues.get("content_ratio", ""),
                        "clean_ratio_low": page_cues.get("clean_ratio_low", ""),
                        "clean_ratio_high": page_cues.get("clean_ratio_high", ""),
                        "content_ratio_in_clean_band": page_cues.get("content_ratio_in_clean_band", ""),
                        "boundary_method": page_cues.get("boundary_method", ""),
                        "boundary_reliable": page_cues.get("ratio_guided_boundary_reliable", ""),
                        "ratio_guided_left_ratio": page_cues.get("ratio_guided_left_ratio", ""),
                        "right_boundary_x": page_cues.get("ratio_guided_boundary_x", ""),
                        "right_of_boundary_dark_ratio": page_cues.get("right_of_boundary_dark_ratio", ""),
                        "right_of_boundary_colmax_ratio": page_cues.get("right_of_boundary_colmax_ratio", ""),
                        "right_strip_dark_ratio_6": page_cues.get("right_strip_dark_ratio_6", ""),
                        "right_strip_peak_cols_6": page_cues.get("right_strip_peak_cols_6", ""),
                        "right_strip_dark_ratio_8": page_cues.get("right_strip_dark_ratio_8", ""),
                        "right_strip_peak_cols_8": page_cues.get("right_strip_peak_cols_8", ""),
                        "right_strip_dark_ratio_10": page_cues.get("right_strip_dark_ratio_10", ""),
                        "right_strip_peak_cols_10": page_cues.get("right_strip_peak_cols_10", ""),
                        "subtle_right_strip_suspect": page_cues.get("subtle_right_strip_suspect", ""),
                    }
                    if rec.get("decision") == "uncertain":
                        self.log("Page result (uncertain): " + summarize_page_result(rec))
                    else:
                        self.log("Page result: " + summarize_page_result(rec))
                records.append(rec)
                file_records.append(rec)
                if on_page_result:
                    try:
                        on_page_result(rec, pdf_path, doc)
                    except Exception as cb_exc:
                        self.log(f"on_page_result callback failed: {cb_exc}")
                done += 1
                self.progress_cb(done, max(total_pages, 1))

            if on_file_done:
                try:
                    on_file_done(pdf_path, doc, file_records)
                except Exception as cb_exc:
                    self.log(f"on_file_done callback failed: {cb_exc}")
            doc.close()

        return records


class PythonHeuristicEngine:
    def __init__(
        self,
        memory: Dict[str, Any],
        logger,
        cancel_event: threading.Event,
        pause_event: Optional[threading.Event],
        progress_cb,
        render_dpi: int = 220,
        parameter_override: Optional[Dict[str, float]] = None,
    ) -> None:
        self.memory = ensure_memory_schema(memory)
        self.log = logger
        self.cancel_event = cancel_event
        self.pause_event = pause_event
        self.progress_cb = progress_cb
        self.render_dpi = max(120, min(360, int(render_dpi)))
        self.batch_width_info: Dict[str, Any] = {}
        self.parameter_override = parameter_override or {}

    def memory_override(self, file_name: str, page_no: int) -> Optional[Dict[str, Any]]:
        key = f"{file_name.lower()}::{page_no}"
        return self.memory.get("overrides", {}).get(key)

    def _wait_if_paused(self) -> None:
        while self.pause_event and self.pause_event.is_set() and not self.cancel_event.is_set():
            time.sleep(0.15)

    def _apply_parameter_override(self, info: Dict[str, Any]) -> Dict[str, Any]:
        info = dict(info or {})
        normal_width = float(self.parameter_override.get("normal_width") or 0.0)
        overlap_multiplier = float(self.parameter_override.get("overlap_multiplier") or 0.0)
        if normal_width > 0:
            info["baseline_body_width"] = round(normal_width, 3)
        effective_normal = float(info.get("baseline_body_width") or 0.0)
        if overlap_multiplier > 0:
            info["body_width_overlap_rel_threshold"] = round(overlap_multiplier, 4)
        if effective_normal > 0:
            rel = float(info.get("body_width_overlap_rel_threshold") or PY_WIDTH_OVERLAP_REL_THRESHOLD)
            info["body_width_overlap_threshold"] = round(effective_normal * rel, 3)
        return info

    def scan_pdfs(
        self,
        pdf_paths: List[Path],
        scope: str,
        custom_prompt: str,
        on_page_result: Optional[Callable[[Dict[str, Any], Path, fitz.Document], None]] = None,
        on_file_done: Optional[Callable[[Path, fitz.Document, List[Dict[str, Any]]], None]] = None,
    ) -> List[Dict[str, Any]]:
        del custom_prompt
        records: List[Dict[str, Any]] = []
        total_pages = 0
        for p in pdf_paths:
            try:
                doc = fitz.open(str(p))
                total_pages += len(doc)
                doc.close()
            except Exception:
                pass
        done = 0
        self.progress_cb(done, max(total_pages, 1))

        if pdf_paths and (scope == "source" or not self.batch_width_info):
            try:
                self.log(f"Estimating normal page width for {scope} pages...")
                self.batch_width_info = estimate_width_baseline(
                    pdf_paths,
                    dpi=self.render_dpi,
                    max_width=100000,
                    quality=55,
                )
                self.batch_width_info = self._apply_parameter_override(self.batch_width_info)
                self.log(
                    "Width baseline: "
                    f"normal_width={self.batch_width_info.get('baseline_body_width')} "
                    f"overlap_width={self.batch_width_info.get('body_width_overlap_threshold')} "
                    f"multiplier={self.batch_width_info.get('body_width_overlap_rel_threshold')} "
                    f"pages={self.batch_width_info.get('page_count')}"
                )
            except Exception as exc:
                self.batch_width_info = {
                    "baseline_body_width": 0.0,
                    "body_width_overlap_rel_threshold": PY_WIDTH_OVERLAP_REL_THRESHOLD,
                    "body_width_overlap_threshold": 0.0,
                    "page_count": 0,
                }
                self.batch_width_info = self._apply_parameter_override(self.batch_width_info)
                self.log(f"Width baseline estimation failed; using configured values only: {exc}")
        elif self.batch_width_info:
            self.log(
                "Reusing width baseline: "
                f"normal_width={self.batch_width_info.get('baseline_body_width')} "
                f"overlap_width={self.batch_width_info.get('body_width_overlap_threshold')}"
            )

        for pdf_path in pdf_paths:
            self._wait_if_paused()
            if self.cancel_event.is_set():
                self.log("Scan cancelled.")
                break

            self.log(f"Scanning: {pdf_path}")
            try:
                doc = fitz.open(str(pdf_path))
            except Exception as exc:
                self.log(f"Failed to open {pdf_path}: {exc}")
                continue
            file_records: List[Dict[str, Any]] = []

            for idx in range(len(doc)):
                self._wait_if_paused()
                if self.cancel_event.is_set():
                    self.log("Scan cancelled.")
                    break

                page_no = idx + 1
                file_name = pdf_path.name
                override = self.memory_override(file_name, page_no)
                if override:
                    override_decision = str(override.get("decision", "")).strip().lower()
                    if override_decision not in {"overlap", "blurry", "clean", "uncertain"}:
                        override_overlap = bool(override.get("is_overlap", False))
                        override_blurry = bool(override.get("is_blurry", False))
                        override_decision = "overlap" if override_overlap else ("blurry" if override_blurry else "clean")
                    override_overlap = bool(override.get("is_overlap", override_decision == "overlap"))
                    override_blurry = bool(override.get("is_blurry", override_decision == "blurry"))
                    rec = {
                        "source_directory": str(pdf_path.parent),
                        "file_name": file_name,
                        "file_path": str(pdf_path),
                        "page": page_no,
                        "decision": override_decision,
                        "is_overlap": override_overlap,
                        "is_blurry": override_blurry,
                        "confidence": float(override.get("confidence", 1.0)),
                        "overlap_type": str(override.get("overlap_type", "manual_override")),
                        "signatures": override.get("signatures", []),
                        "reason": str(override.get("note", "manual memory override")),
                        "scope": scope,
                        "status": "memory_override",
                        "error_detail": "",
                        "trimmed_body_width": "",
                    }
                    records.append(rec)
                    file_records.append(rec)
                    self.log("Page result (memory): " + summarize_page_result(rec))
                    if on_page_result:
                        try:
                            on_page_result(rec, pdf_path, doc)
                        except Exception as cb_exc:
                            self.log(f"on_page_result callback failed: {cb_exc}")
                    done += 1
                    self.progress_cb(done, max(total_pages, 1))
                    continue

                try:
                    image_jpeg = render_page_jpeg(doc[idx], dpi=self.render_dpi, max_width=100000)
                    page_cues = measure_page_visual_cues(image_jpeg)
                    page_cues = enrich_python_width_cues(page_cues, self.batch_width_info)
                    blurry_stats = compute_blurry_stats(image_jpeg)
                    py_result = classify_python_page(page_cues, blurry_stats)
                    rec = {
                        "source_directory": str(pdf_path.parent),
                        "file_name": file_name,
                        "file_path": str(pdf_path),
                        "page": page_no,
                        "decision": py_result.get("decision", "uncertain"),
                        "is_overlap": bool(py_result.get("is_overlap", False)),
                        "is_blurry": bool(py_result.get("is_blurry", False)),
                        "confidence": float(py_result.get("confidence", 0.0) or 0.0),
                        "overlap_type": str(py_result.get("overlap_type", "none")),
                        "signatures": list(py_result.get("signatures", [])),
                        "reason": str(py_result.get("reason", ""))[:500],
                        "scope": scope,
                        "status": "ok",
                        "error_detail": "",
                        "trimmed_body_width": page_cues.get("trimmed_body_width", ""),
                    }
                    rec["reason"] = (
                        f"{rec['reason']} | blur_edge={blurry_stats.get('blur_edge_energy')} "
                        f"blur_contrast={blurry_stats.get('blur_contrast_stddev')}"
                    )[:500]
                except Exception as exc:
                    self.log(f"Python heuristic failed {pdf_path} p{page_no}: {exc}")
                    rec = {
                        "source_directory": str(pdf_path.parent),
                        "file_name": file_name,
                        "file_path": str(pdf_path),
                        "page": page_no,
                        "decision": "uncertain",
                        "is_overlap": False,
                        "is_blurry": False,
                        "confidence": 0.0,
                        "overlap_type": "none",
                        "signatures": [],
                        "reason": f"python_error: {exc}",
                        "scope": scope,
                        "status": "error",
                        "error_detail": f"python_error: {exc}",
                        "trimmed_body_width": "",
                    }

                records.append(rec)
                file_records.append(rec)
                if rec.get("decision") == "uncertain":
                    self.log("Page result (uncertain): " + summarize_page_result(rec))
                else:
                    self.log("Page result: " + summarize_page_result(rec))
                if on_page_result:
                    try:
                        on_page_result(rec, pdf_path, doc)
                    except Exception as cb_exc:
                        self.log(f"on_page_result callback failed: {cb_exc}")
                done += 1
                self.progress_cb(done, max(total_pages, 1))

            if on_file_done:
                try:
                    on_file_done(pdf_path, doc, file_records)
                except Exception as cb_exc:
                    self.log(f"on_file_done callback failed: {cb_exc}")
            doc.close()

        return records


def write_overlap_csv(records: List[Dict[str, Any]], out_csv: Path) -> int:
    overlaps = [
        r
        for r in records
        if r.get("scope") == "source"
        and (r.get("is_overlap") or r.get("is_blurry") or r.get("decision") == "uncertain")
    ]
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with out_csv.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=OVERLAP_CSV_FIELDS)
        w.writeheader()
        for r in overlaps:
            w.writerow(overlap_row_for_csv(r))
    return len(overlaps)


def write_source_csv(records: List[Dict[str, Any]], out_csv: Path) -> int:
    source_rows = [
        r
        for r in records
        if r.get("scope") == "source"
        and (r.get("is_overlap") or r.get("is_blurry") or r.get("decision") == "uncertain")
    ]
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with out_csv.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=OVERLAP_CSV_FIELDS)
        w.writeheader()
        for r in source_rows:
            w.writerow(overlap_row_for_csv(r))
    return len(source_rows)


def overlap_only_csv_path(out_csv: Path) -> Path:
    return out_csv.with_name(f"{out_csv.stem}_overlaps_only{out_csv.suffix}")


def build_prefixed_output_name(prefix: str, src_stem: str, page: int, naming_meta: Optional[Dict[str, str]] = None) -> str:
    naming_meta = naming_meta or {}
    batch = str(naming_meta.get("batch", "")).strip()
    tray = str(naming_meta.get("tray", "")).strip()
    slot = str(naming_meta.get("slot", "")).strip()
    archive = str(naming_meta.get("archive", "")).strip()
    if prefix == "U":
        return f"U_{src_stem}_P{page}.pdf"
    parts: List[str] = []
    if prefix == "B":
        parts.append("Blurry")
    if batch:
        parts.append(f"B{batch}")
    else:
        parts.append("B")
    if tray:
        parts.append(f"T{tray}")
    if slot:
        parts.append(slot)
    if archive:
        parts.append(f"A{archive}")
    parts.append(src_stem)
    parts.append(f"P{page}")
    return "_".join(parts) + ".pdf"


def export_single_tagged_page_from_doc(
    doc: fitz.Document,
    src_path: Path,
    page: int,
    prefix: str,
    logger,
    output_dir: Optional[Path] = None,
    naming_meta: Optional[Dict[str, str]] = None,
) -> bool:
    out_root = output_dir or src_path.parent
    out_root.mkdir(parents=True, exist_ok=True)
    out = out_root / build_prefixed_output_name(prefix, src_path.stem, page, naming_meta=naming_meta)
    try:
        one = fitz.open()
        one.insert_pdf(doc, from_page=page - 1, to_page=page - 1)
        one.save(str(out))
        one.close()
        return True
    except Exception as exc:
        logger(f"Export {prefix}_ page failed {src_path.name} p{page}: {exc}")
        return False


def export_single_overlap_page_from_doc(
    doc: fitz.Document,
    src_path: Path,
    page: int,
    logger,
    output_dir: Optional[Path] = None,
    naming_meta: Optional[Dict[str, str]] = None,
) -> bool:
    return export_single_tagged_page_from_doc(doc, src_path, page, "O", logger, output_dir=output_dir, naming_meta=naming_meta)


def export_single_blurry_page_from_doc(
    doc: fitz.Document,
    src_path: Path,
    page: int,
    logger,
    output_dir: Optional[Path] = None,
    naming_meta: Optional[Dict[str, str]] = None,
) -> bool:
    return export_single_tagged_page_from_doc(doc, src_path, page, "B", logger, output_dir=output_dir, naming_meta=naming_meta)


def export_single_uncertain_page_from_doc(
    doc: fitz.Document, src_path: Path, page: int, logger, output_dir: Optional[Path] = None
) -> bool:
    return export_single_tagged_page_from_doc(doc, src_path, page, "U", logger, output_dir=output_dir)


def export_extracted_non_overlap_for_file(
    doc: fitz.Document,
    src_path: Path,
    file_records: List[Dict[str, Any]],
    logger,
    output_dir: Optional[Path] = None,
) -> bool:
    marks: Dict[int, bool] = {}
    for r in file_records:
        if r.get("scope") != "source":
            continue
        marks[int(r["page"])] = bool(r.get("is_overlap") or r.get("is_blurry"))

    keep_pages = [p for p, is_flagged in sorted(marks.items()) if not is_flagged]
    if not keep_pages:
        logger(f"No clean pages for {src_path.name}, skip E_ output.")
        return False

    out_root = output_dir or src_path.parent
    out_root.mkdir(parents=True, exist_ok=True)
    out = out_root / f"E_{src_path.name}"
    try:
        out_doc = fitz.open()
        for p in keep_pages:
            out_doc.insert_pdf(doc, from_page=p - 1, to_page=p - 1)
        out_doc.save(str(out))
        out_doc.close()
        return True
    except Exception as exc:
        logger(f"Create E_ file failed {src_path.name}: {exc}")
        return False


def export_overlap_pages(records: List[Dict[str, Any]], logger, output_dir: Optional[Path] = None, naming_meta: Optional[Dict[str, str]] = None) -> int:
    targets = [r for r in records if r.get("scope") == "source" and r.get("is_overlap")]
    by_file: Dict[str, List[int]] = {}
    for r in targets:
        by_file.setdefault(r["file_path"], []).append(int(r["page"]))

    created = 0
    for file_path, pages in by_file.items():
        src = Path(file_path)
        try:
            doc = fitz.open(str(src))
        except Exception as exc:
            logger(f"Open failed {src}: {exc}")
            continue
        for page in sorted(set(pages)):
            try:
                if export_single_overlap_page_from_doc(doc, src, page, logger, output_dir=output_dir, naming_meta=naming_meta):
                    created += 1
            except Exception as exc:
                logger(f"Export overlap page failed {src.name} p{page}: {exc}")
        doc.close()
    return created


def export_blurry_pages(records: List[Dict[str, Any]], logger, output_dir: Optional[Path] = None, naming_meta: Optional[Dict[str, str]] = None) -> int:
    targets = [r for r in records if r.get("scope") == "source" and r.get("is_blurry")]
    by_file: Dict[str, List[int]] = {}
    for r in targets:
        by_file.setdefault(r["file_path"], []).append(int(r["page"]))

    created = 0
    for file_path, pages in by_file.items():
        src = Path(file_path)
        try:
            doc = fitz.open(str(src))
        except Exception as exc:
            logger(f"Open failed {src}: {exc}")
            continue
        for page in sorted(set(pages)):
            if export_single_blurry_page_from_doc(doc, src, page, logger, output_dir=output_dir, naming_meta=naming_meta):
                created += 1
        doc.close()
    return created


def export_uncertain_pages(records: List[Dict[str, Any]], logger, output_dir: Optional[Path] = None) -> int:
    targets = [r for r in records if r.get("scope") == "source" and r.get("decision") == "uncertain"]
    by_file: Dict[str, List[int]] = {}
    for r in targets:
        by_file.setdefault(r["file_path"], []).append(int(r["page"]))

    created = 0
    for file_path, pages in by_file.items():
        src = Path(file_path)
        try:
            doc = fitz.open(str(src))
        except Exception as exc:
            logger(f"Open failed {src}: {exc}")
            continue
        for page in sorted(set(pages)):
            if export_single_uncertain_page_from_doc(doc, src, page, logger, output_dir=output_dir):
                created += 1
        doc.close()
    return created


def export_extracted_non_overlap(records: List[Dict[str, Any]], logger, output_dir: Optional[Path] = None) -> int:
    by_file: Dict[str, Dict[int, bool]] = {}
    for r in records:
        if r.get("scope") != "source":
            continue
        by_file.setdefault(r["file_path"], {})[int(r["page"])] = bool(
            r.get("is_overlap") or r.get("is_blurry")
        )

    created = 0
    for file_path, marks in by_file.items():
        src = Path(file_path)
        try:
            doc = fitz.open(str(src))
        except Exception as exc:
            logger(f"Open failed {src}: {exc}")
            continue
        keep_pages = [p for p, is_flagged in sorted(marks.items()) if not is_flagged]
        if not keep_pages:
            logger(f"No clean pages for {src.name}, skip E_ output.")
            doc.close()
            continue
        out_root = output_dir or src.parent
        out_root.mkdir(parents=True, exist_ok=True)
        out = out_root / f"E_{src.name}"
        try:
            out_doc = fitz.open()
            for p in keep_pages:
                out_doc.insert_pdf(doc, from_page=p - 1, to_page=p - 1)
            out_doc.save(str(out))
            out_doc.close()
            created += 1
        except Exception as exc:
            logger(f"Create E_ file failed {src.name}: {exc}")
        doc.close()
    return created


def find_best_replacement(
    overlap_rec: Dict[str, Any],
    candidate_recs: List[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    target_sigs = [sig_tokens(s) for s in overlap_rec.get("signatures", []) if s]
    if not target_sigs:
        return None

    best: Optional[Dict[str, Any]] = None
    best_score = 0.0
    for c in candidate_recs:
        if c.get("is_overlap") or c.get("is_blurry"):
            continue
        if c.get("file_path") == overlap_rec.get("file_path") and int(c.get("page", 0)) == int(
            overlap_rec.get("page", 0)
        ):
            continue
        cand_sigs = [sig_tokens(s) for s in c.get("signatures", []) if s]
        if not cand_sigs:
            continue

        score = 0.0
        for t in target_sigs:
            for cs in cand_sigs:
                score = max(score, jaccard(t, cs))

        # Slight preference for high-confidence non-overlap candidates.
        score += min(0.2, max(0.0, float(c.get("confidence", 0.0)) * 0.2))

        if score > best_score:
            best_score = score
            best = c

    if best_score < 0.35:
        return None
    return best


def replace_overlap_pages(
    source_records: List[Dict[str, Any]],
    all_records: List[Dict[str, Any]],
    logger,
) -> Tuple[int, Path]:
    source_by_file: Dict[str, List[Dict[str, Any]]] = {}
    for r in source_records:
        source_by_file.setdefault(r["file_path"], []).append(r)

    candidates = [r for r in all_records if not r.get("is_overlap") and not r.get("is_blurry")]

    report_rows: List[Dict[str, Any]] = []
    replaced_files = 0
    doc_cache: Dict[str, fitz.Document] = {}

    def get_doc(path: str) -> fitz.Document:
        if path not in doc_cache:
            doc_cache[path] = fitz.open(path)
        return doc_cache[path]

    for file_path, recs in source_by_file.items():
        src = Path(file_path)
        overlap_pages = [r for r in recs if r.get("is_overlap")]
        if not overlap_pages:
            continue
        page_replacements: Dict[int, Optional[Dict[str, Any]]] = {}
        for ov in overlap_pages:
            cand = find_best_replacement(ov, candidates)
            page_replacements[int(ov["page"])] = cand
            report_rows.append(
                {
                    "source_file": str(src),
                    "source_page": int(ov["page"]),
                    "source_signatures": " | ".join(ov.get("signatures", [])),
                    "replacement_file": cand.get("file_path") if cand else "",
                    "replacement_page": int(cand.get("page", 0)) if cand else "",
                    "status": "replaced" if cand else "not_found",
                }
            )

        try:
            src_doc = get_doc(str(src))
            out_doc = fitz.open()
            for i in range(len(src_doc)):
                p = i + 1
                cand = page_replacements.get(p)
                if cand:
                    cand_doc = get_doc(cand["file_path"])
                    out_doc.insert_pdf(cand_doc, from_page=int(cand["page"]) - 1, to_page=int(cand["page"]) - 1)
                else:
                    out_doc.insert_pdf(src_doc, from_page=i, to_page=i)
            out = src.parent / f"R_{src.name}"
            out_doc.save(str(out))
            out_doc.close()
            replaced_files += 1
        except Exception as exc:
            logger(f"Replacement failed for {src.name}: {exc}")

    for d in doc_cache.values():
        try:
            d.close()
        except Exception:
            pass

    report_path = app_data_dir() / f"replacement_report_{now_file_ts()}.csv"
    with report_path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(
            fh,
            fieldnames=[
                "source_file",
                "source_page",
                "source_signatures",
                "replacement_file",
                "replacement_page",
                "status",
            ],
        )
        w.writeheader()
        w.writerows(report_rows)

    return replaced_files, report_path


def import_training_csv(memory: Dict[str, Any], csv_path: Path) -> Tuple[int, int]:
    ensure_memory_schema(memory)
    added = 0
    notes_added = 0
    overrides = memory.setdefault("overrides", {})
    notes = memory.setdefault("global_notes", [])
    history = memory.setdefault("correction_history", [])
    with csv_path.open("r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            file_name = (
                row.get("file_name")
                or row.get("file")
                or Path(row.get("file_path", "")).name
                or ""
            ).strip()
            page_raw = row.get("page") or row.get("source_page") or ""
            lbl_raw = row.get("is_overlap") or row.get("label") or ""
            note = (row.get("note") or row.get("reason") or "").strip()
            if not file_name:
                continue
            try:
                page = int(str(page_raw).strip())
            except Exception:
                continue

            lbl_s = str(lbl_raw).strip().lower()
            is_overlap = lbl_s in {"1", "true", "yes", "y", "overlap", "ov"}
            is_blurry = lbl_s in {"blurry", "blur", "unreadable"}
            decision = "overlap" if is_overlap else ("blurry" if is_blurry else "clean")
            key = f"{file_name.lower()}::{page}"
            overrides[key] = {
                "decision": decision,
                "is_overlap": is_overlap,
                "is_blurry": is_blurry,
                "confidence": 1.0,
                "overlap_type": "manual_override",
                "signatures": [],
                "note": note or "imported training label",
                "updated_at": now_ts(),
            }
            history.append(
                {
                    "file_name": file_name,
                    "file_path": str(row.get("file_path", "")),
                    "page": page,
                    "previous_decision": "unknown",
                    "corrected_decision": decision,
                    "note": note or "imported training label",
                    "signatures": [],
                    "overlap_type": "manual_override",
                    "updated_at": now_ts(),
                }
            )
            added += 1
            if note and note not in notes:
                notes.append(note)
                notes_added += 1
    if len(history) > 300:
        del history[:-300]
    return added, notes_added


class App(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title(APP_NAME)
        self.geometry("520x820")
        self.minsize(480, 740)

        self.storage = Storage()
        self.models = self.storage.load_models()
        self.memory = ensure_memory_schema(self.storage.load_memory())

        self.log_queue: queue.Queue[str] = queue.Queue()
        self.cancel_event = threading.Event()
        self.pause_event = threading.Event()
        self.worker_thread: Optional[threading.Thread] = None

        self._setup_fonts()
        self._configure_styles()
        self._build_ui()
        self._load_defaults()
        self.after(150, self._drain_logs)

    def _setup_fonts(self) -> None:
        family = "TkDefaultFont"
        try:
            available = {name.lower(): name for name in tkfont.families()}
            family = available.get("roboto", family)
        except Exception:
            pass
        self.font_ui = tkfont.Font(family=family, size=10)
        self.font_small = tkfont.Font(family=family, size=9)
        self.font_label = tkfont.Font(family=family, size=10)
        self.font_title = tkfont.Font(family=family, size=10)
        self.font_heading = tkfont.Font(family=family, size=10)
        self.font_overline = tkfont.Font(family=family, size=10)
        self.font_status = tkfont.Font(family=family, size=10)
        self.font_caption = tkfont.Font(family=family, size=9)

    def _configure_styles(self) -> None:
        style = ttk.Style(self)
        try:
            style.theme_use("default")
        except Exception:
            pass
        self.option_add("*Font", self.font_ui)
        self.option_add("*TCombobox*Listbox.font", self.font_ui)
        style.configure("TButton", padding=(8, 4))
        style.configure("TEntry", padding=(4, 2))
        style.configure("TCombobox", padding=(4, 2))

    def _build_backdrop(self) -> None:
        return

    def _on_root_configure(self, event: tk.Event) -> None:
        return

    def _draw_backdrop(self) -> None:
        return

    def _create_card(self, parent: tk.Widget, title: str, subtitle: str = "", badge: str = "") -> Tuple[tk.Frame, tk.Frame]:
        card = tk.Frame(parent, bg=self.ui["card"], highlightbackground=self.ui["line"], highlightthickness=1, bd=0)
        header = tk.Frame(card, bg=self.ui["card"])
        header.pack(fill=tk.X, padx=18, pady=(16, 10))

        title_box = tk.Frame(header, bg=self.ui["card"])
        title_box.pack(side=tk.LEFT, fill=tk.X, expand=True)
        tk.Label(title_box, text=title, font=self.font_heading, fg=self.ui["ink"], bg=self.ui["card"]).pack(anchor="w")
        if subtitle:
            tk.Label(
                title_box,
                text=subtitle,
                font=self.font_caption,
                fg=self.ui["muted"],
                bg=self.ui["card"],
                wraplength=720,
                justify="left",
            ).pack(anchor="w", pady=(4, 0))
        if badge:
            self._make_pill(header, badge, self.ui["accent_soft"], self.ui["accent"]).pack(side=tk.RIGHT, padx=(12, 0))

        body = tk.Frame(card, bg=self.ui["card"])
        body.pack(fill=tk.BOTH, expand=True, padx=18, pady=(0, 18))
        return card, body

    def _create_soft_panel(self, parent: tk.Widget, title: str) -> tk.Frame:
        panel = tk.Frame(parent, bg=self.ui["card_soft"], highlightbackground=self.ui["line_soft"], highlightthickness=1, bd=0)
        tk.Label(panel, text=title, font=self.font_label, fg=self.ui["ink_soft"], bg=self.ui["card_soft"]).pack(anchor="w", padx=14, pady=(12, 8))
        return panel

    def _make_pill(self, parent: tk.Widget, text: str, bg: str, fg: str) -> tk.Label:
        return tk.Label(
            parent,
            text=text,
            font=self.font_small,
            fg=fg,
            bg=bg,
            padx=10,
            pady=5,
            bd=0,
        )

    def _make_field_label(self, parent: tk.Widget, text: str) -> tk.Label:
        return tk.Label(parent, text=text, font=self.font_label)

    def _make_check(
        self,
        parent: tk.Widget,
        text: str,
        variable: tk.BooleanVar,
        command: Optional[Callable[[], None]] = None,
    ) -> tk.Checkbutton:
        try:
            bg = str(parent.cget("background"))
        except Exception:
            bg = str(self.cget("bg"))
        return tk.Checkbutton(
            parent,
            text=text,
            variable=variable,
            command=command,
            font=self.font_ui,
            bg=bg,
            activebackground=bg,
            selectcolor=bg,
            highlightthickness=0,
            bd=0,
            relief="flat",
        )

    def _style_text_widget(self, widget: ScrolledText) -> None:
        widget.configure(
            relief="sunken",
            bd=1,
            padx=6,
            pady=6,
            wrap=tk.CHAR,
        )

    def _set_status(self, text: str) -> None:
        self.after(0, lambda: self.status_var.set(text))

    def _build_ui(self) -> None:
        root = ttk.Frame(self, padding=12)
        root.pack(fill=tk.BOTH, expand=True)
        root.columnconfigure(1, weight=1)
        root.rowconfigure(4, weight=1)

        self.csv_path_var = tk.StringVar()
        self.source_dir_var = tk.StringVar()
        self.output_dir_var = tk.StringVar()
        self.param_unlock_var = tk.BooleanVar(value=False)
        self.param_normal_width_var = tk.StringVar(value="")
        self.param_overlap_multiplier_var = tk.StringVar(value="")
        self.estimated_normal_width_var = tk.StringVar(value="Estimated normal: -")
        self.name_batch_var = tk.StringVar(value="")
        self.name_tray_var = tk.StringVar(value="")
        self.name_slot_var = tk.StringVar(value="")
        self.name_archive_var = tk.StringVar(value="")

        source_box = ttk.LabelFrame(root, text="Source")
        source_box.grid(row=0, column=0, columnspan=2, sticky="nsew")
        source_box.columnconfigure(1, weight=1)

        ttk.Label(source_box, text="Scan Directory").grid(row=0, column=0, sticky="w", padx=8, pady=(8, 4))
        self.source_dir_entry = ttk.Entry(source_box, textvariable=self.source_dir_var)
        self.source_dir_entry.grid(row=0, column=1, sticky="ew", padx=8, pady=(8, 4))
        ttk.Button(source_box, text="Browse", command=self.pick_source_dir).grid(row=0, column=2, sticky="ew", padx=(0, 8), pady=(8, 4))

        ttk.Label(source_box, text="Output Directory").grid(row=1, column=0, sticky="w", padx=8, pady=(4, 8))
        self.output_dir_entry = ttk.Entry(source_box, textvariable=self.output_dir_var)
        self.output_dir_entry.grid(row=1, column=1, sticky="ew", padx=8, pady=(4, 8))
        ttk.Button(source_box, text="Browse", command=self.pick_output_dir).grid(row=1, column=2, sticky="ew", padx=(0, 8), pady=(4, 8))

        options_box = ttk.LabelFrame(root, text="Outputs")
        options_box.grid(row=1, column=0, columnspan=2, sticky="nsew", pady=(10, 0))
        options_box.columnconfigure(0, weight=1)
        options_box.columnconfigure(1, weight=1)

        self.recursive_var = tk.BooleanVar(value=True)
        self.live_output_var = tk.BooleanVar(value=True)
        self.act_csv_var = tk.BooleanVar(value=True)
        self.act_identify_var = tk.BooleanVar(value=True)
        self.act_extract_var = tk.BooleanVar(value=True)
        self.act_replace_var = tk.BooleanVar(value=False)
        self.act_blurry_var = tk.BooleanVar(value=True)
        self.act_uncertain_var = tk.BooleanVar(value=True)

        self._make_check(options_box, "CSV", self.act_csv_var).grid(row=0, column=0, sticky="w", padx=8, pady=(8, 2))
        self._make_check(options_box, "Overlap", self.act_identify_var).grid(row=0, column=1, sticky="w", padx=8, pady=(8, 2))
        self._make_check(options_box, "Blurry", self.act_blurry_var).grid(row=1, column=0, sticky="w", padx=8, pady=2)
        self._make_check(options_box, "Extracted Original", self.act_extract_var).grid(row=1, column=1, sticky="w", padx=8, pady=2)

        param_box = ttk.LabelFrame(root, text="Parameters")
        param_box.grid(row=2, column=0, columnspan=2, sticky="nsew", pady=(10, 0))
        param_box.columnconfigure(1, weight=1)
        param_box.columnconfigure(3, weight=1)
        self._make_check(param_box, "Unlock", self.param_unlock_var, command=self._toggle_parameter_inputs).grid(
            row=0, column=0, sticky="w", padx=8, pady=(8, 4)
        )
        ttk.Button(param_box, text="Estimate", command=self.estimate_normal_width).grid(
            row=0, column=1, sticky="w", padx=(0, 8), pady=(8, 4)
        )
        ttk.Label(param_box, text="Normal Width").grid(row=1, column=0, sticky="w", padx=8, pady=(4, 8))
        self.param_normal_entry = ttk.Entry(param_box, textvariable=self.param_normal_width_var, width=12)
        self.param_normal_entry.grid(row=1, column=1, sticky="w", padx=(0, 18), pady=(4, 8))
        ttk.Label(param_box, text="Overlap Multiplier").grid(row=1, column=2, sticky="w", padx=8, pady=(4, 8))
        self.param_overlap_entry = ttk.Entry(param_box, textvariable=self.param_overlap_multiplier_var, width=12)
        self.param_overlap_entry.grid(row=1, column=3, sticky="w", padx=(0, 8), pady=(4, 8))
        ttk.Label(param_box, textvariable=self.estimated_normal_width_var).grid(
            row=2, column=0, columnspan=4, sticky="w", padx=8, pady=(0, 8)
        )

        ttk.Label(options_box, text="Batch").grid(row=2, column=0, sticky="w", padx=8, pady=(8, 2))
        ttk.Label(options_box, text="Tray").grid(row=2, column=1, sticky="w", padx=8, pady=(8, 2))
        self.batch_entry = ttk.Entry(options_box, textvariable=self.name_batch_var, width=10)
        self.batch_entry.grid(row=3, column=0, sticky="w", padx=8, pady=(0, 4))
        self.tray_entry = ttk.Entry(options_box, textvariable=self.name_tray_var, width=10)
        self.tray_entry.grid(row=3, column=1, sticky="w", padx=8, pady=(0, 4))
        ttk.Label(options_box, text="Slot").grid(row=4, column=0, sticky="w", padx=8, pady=(4, 2))
        ttk.Label(options_box, text="Archive").grid(row=4, column=1, sticky="w", padx=8, pady=(4, 2))
        self.slot_entry = ttk.Entry(options_box, textvariable=self.name_slot_var, width=10)
        self.slot_entry.grid(row=5, column=0, sticky="w", padx=8, pady=(0, 8))
        self.archive_entry = ttk.Entry(options_box, textvariable=self.name_archive_var, width=10)
        self.archive_entry.grid(row=5, column=1, sticky="w", padx=8, pady=(0, 8))

        run_box = ttk.LabelFrame(root, text="Run")
        run_box.grid(row=3, column=0, columnspan=2, sticky="ew", pady=(10, 0))
        self.status_var = tk.StringVar(value="Ready.")
        ttk.Label(run_box, textvariable=self.status_var).pack(anchor="w", padx=8, pady=(8, 4))
        run_buttons = ttk.Frame(run_box)
        run_buttons.pack(fill=tk.X, padx=8, pady=(0, 4))
        self.run_btn = ttk.Button(run_buttons, text="Run", command=self.run_pipeline)
        self.run_btn.pack(side=tk.LEFT)
        self.pause_btn_text = tk.StringVar(value="Pause")
        self.pause_btn = ttk.Button(run_buttons, textvariable=self.pause_btn_text, command=self.pause_pipeline)
        self.pause_btn.pack(side=tk.LEFT, padx=(8, 0))
        self.stop_btn = ttk.Button(run_buttons, text="Stop", command=self.stop_pipeline)
        self.stop_btn.pack(side=tk.LEFT, padx=(8, 0))
        self.progress = ttk.Progressbar(run_box, mode="determinate")
        self.progress.pack(fill=tk.X, padx=8, pady=(0, 8))

        log_box = ttk.LabelFrame(root, text="Running Log")
        log_box.grid(row=4, column=0, columnspan=2, sticky="nsew", pady=(10, 0))
        log_box.columnconfigure(0, weight=1)
        log_box.rowconfigure(0, weight=1)
        self.log_text = ScrolledText(log_box, height=18)
        self.log_text.grid(row=0, column=0, sticky="nsew", padx=8, pady=8)
        self._style_text_widget(self.log_text)

    def _load_defaults(self) -> None:
        default_dir = str(Path.cwd())
        self.source_dir_var.set(default_dir)
        self.output_dir_var.set(default_dir)
        self.csv_path_var.set(str(Path(default_dir) / f"overlap_report_{now_file_ts()}.csv"))
        self.param_normal_width_var.set(f"{DEFAULT_NORMAL_BODY_WIDTH:.0f}")
        self.param_overlap_multiplier_var.set(f"{PY_WIDTH_OVERLAP_REL_THRESHOLD:.2f}")
        self.estimated_normal_width_var.set("Estimated normal: -")
        self._toggle_parameter_inputs()

    def _refresh_memory_info(self) -> None:
        if not hasattr(self, "memory_info_var"):
            return
        notes = len(self.memory.get("global_notes", []))
        overrides = len(self.memory.get("overrides", {}))
        corrections = len(self.memory.get("correction_history", []))
        self.memory_info_var.set(f"Memory: {notes} notes, {overrides} overrides, {corrections} corrections")

    def toggle_sensitive_fields(self) -> None:
        return

    def lock_profile_config_ui(self, config_path: Path) -> None:
        return

    def _toggle_parameter_inputs(self) -> None:
        state = "normal" if self.param_unlock_var.get() else "disabled"
        self.param_normal_entry.configure(state=state)
        self.param_overlap_entry.configure(state=state)

    def _parameter_override(self) -> Dict[str, float]:
        out: Dict[str, float] = {}
        try:
            normal = float(self.param_normal_width_var.get().strip())
            if normal > 0:
                out["normal_width"] = normal
        except Exception:
            pass
        try:
            overlap = float(self.param_overlap_multiplier_var.get().strip())
            if overlap > 0:
                out["overlap_multiplier"] = overlap
        except Exception:
            pass
        return out

    def _output_naming_meta(self) -> Dict[str, str]:
        return {
            "batch": self.name_batch_var.get().strip(),
            "tray": self.name_tray_var.get().strip(),
            "slot": self.name_slot_var.get().strip(),
            "archive": self.name_archive_var.get().strip(),
        }

    def estimate_normal_width(self) -> None:
        if self.worker_thread and self.worker_thread.is_alive():
            messagebox.showwarning("Busy", "Wait for the current run to finish.")
            return
        source_dir = Path(self.source_dir_var.get().strip())
        if not source_dir.exists() or not source_dir.is_dir():
            messagebox.showerror("Error", "Source directory does not exist.")
            return

        def worker() -> None:
            try:
                self._set_status("Estimating normal width...")
                self.log("Estimating normal width...")
                pdfs = list_pdfs(source_dir, recursive=True)
                if not pdfs:
                    self._set_status("No source PDFs found.")
                    self.log("No PDF files found in source directory.")
                    return
                info = estimate_width_baseline(pdfs, dpi=220, max_width=100000, quality=55)
                normal_width = float(info.get("baseline_body_width") or 0.0)
                if normal_width > 0:
                    self.after(0, lambda: self.estimated_normal_width_var.set(f"Estimated normal: {normal_width:.0f}"))
                    self._set_status("Estimate complete.")
                    self.log(f"Estimated normal width: {normal_width:.0f}")
                else:
                    self._set_status("Normal width estimate failed.")
                    self.log("Normal width estimate failed.")
            except Exception as exc:
                self._set_status("Normal width estimate failed.")
                self.log(f"Normal width estimate failed: {exc}")

        threading.Thread(target=worker, daemon=True).start()

    def _drain_logs(self) -> None:
        try:
            while True:
                msg = self.log_queue.get_nowait()
                self.log_text.insert(tk.END, f"[{now_ts()}] {msg}\n")
                self.log_text.see(tk.END)
        except queue.Empty:
            pass
        self.after(150, self._drain_logs)

    def log(self, msg: str) -> None:
        self.log_queue.put(msg)

    def get_selected_model_index(self) -> Optional[int]:
        name = self.model_name_var.get().strip()
        for i, m in enumerate(self.models):
            if m.name == name:
                return i
        return None

    def on_select_model(self, *_args) -> None:
        idx = self.get_selected_model_index()
        if idx is None:
            return
        m = self.models[idx]
        self.base_url_var.set(m.base_url)
        self.model_id_var.set(m.model)
        self.api_key_var.set(m.api_key)
        self.timeout_var.set(str(m.timeout_sec))

    def add_model(self) -> None:
        name = simpledialog.askstring("Add Model", "Profile name:", parent=self)
        if not name:
            return
        if any(x.name == name for x in self.models):
            messagebox.showerror("Error", "Profile name already exists.")
            return
        base_url = simpledialog.askstring("Add Model", "Base URL:", parent=self)
        if not base_url:
            return
        model = simpledialog.askstring(
            "Add Model",
            "Model Name / Alias (for built-ins use names like GPT-5.4, Kimi-K2.5; for local endpoints use the local model id):",
            parent=self,
        )
        if not model:
            return
        api_key = simpledialog.askstring("Add Model", "API Key (optional; leave blank for local endpoints):", parent=self, show="*") or ""
        timeout = simpledialog.askinteger("Add Model", "Timeout seconds:", initialvalue=120, parent=self)
        timeout = timeout or 120

        self.models.append(
            ModelProfile(name=name.strip(), base_url=base_url.strip(), model=model.strip(), api_key=api_key, timeout_sec=timeout)
        )
        self.storage.save_models(self.models)
        self.model_combo["values"] = [m.name for m in self.models]
        self.model_name_var.set(name.strip())
        self.on_select_model()
        self.log(f"Added model profile: {name}")

    def delete_model(self) -> None:
        idx = self.get_selected_model_index()
        if idx is None:
            return
        name = self.models[idx].name
        if not messagebox.askyesno("Confirm", f"Delete model profile '{name}'?"):
            return
        del self.models[idx]
        self.storage.save_models(self.models)
        names = [m.name for m in self.models]
        self.model_combo["values"] = names
        if names:
            self.model_combo.current(0)
            self.on_select_model()
        else:
            self.model_name_var.set("")
            self.base_url_var.set("")
            self.model_id_var.set("")
            self.api_key_var.set("")
            self.timeout_var.set("120")
        self.log(f"Deleted model profile: {name}")

    def save_model(self) -> None:
        idx = self.get_selected_model_index()
        if idx is None:
            messagebox.showerror("Error", "Select a model profile first.")
            return
        try:
            timeout = int(self.timeout_var.get().strip())
        except Exception:
            messagebox.showerror("Error", "Timeout must be integer.")
            return
        m = self.models[idx]
        m.base_url = self.base_url_var.get().strip()
        m.model = self.model_id_var.get().strip()
        m.api_key = self.api_key_var.get().strip()
        m.timeout_sec = max(10, timeout)
        self.storage.save_models(self.models)
        self.log(f"Saved model profile: {m.name}")
        messagebox.showinfo("Saved", "Model profile saved.")

    def current_profile(self) -> Optional[ModelProfile]:
        idx = self.get_selected_model_index()
        if idx is None:
            return None
        m = self.models[idx]
        return ModelProfile(
            name=m.name,
            base_url=m.base_url,
            model=m.model,
            api_key=m.api_key,
            timeout_sec=max(10, int(m.timeout_sec)),
        )

    def test_model(self) -> None:
        profile = self.current_profile()
        if not profile:
            messagebox.showerror("Error", "No model selected.")
            return

        def worker() -> None:
            self._set_status(f"Testing model connectivity for {profile.name}...")
            self.log(f"Testing model: {profile.name} / {profile.model}")
            client = OpenAICompatibleClient(profile)
            ok, msg = client.quick_test()
            if ok:
                self._set_status(f"Model test passed for {profile.name}.")
                self.log(f"Model test OK: {msg}")
            else:
                self._set_status(f"Model test failed for {profile.name}. Check log.")
                self.log(f"Model test failed: {msg}")

        threading.Thread(target=worker, daemon=True).start()

    def pick_source_dir(self) -> None:
        d = filedialog.askdirectory(title="Select Source Directory")
        if d:
            self.source_dir_var.set(d)
            if not self.output_dir_var.get().strip():
                self.output_dir_var.set(d)
            self.csv_path_var.set(str(Path(self.output_dir_var.get().strip() or d) / f"overlap_report_{now_file_ts()}.csv"))

    def pick_output_dir(self) -> None:
        d = filedialog.askdirectory(title="Select Output Directory")
        if d:
            self.output_dir_var.set(d)
            self.csv_path_var.set(str(Path(d) / f"overlap_report_{now_file_ts()}.csv"))

    def import_training(self) -> None:
        p = filedialog.askopenfilename(
            title="Select Training CSV",
            filetypes=[("CSV", "*.csv")],
        )
        if not p:
            return
        try:
            added, notes = import_training_csv(self.memory, Path(p))
            self.storage.save_memory(self.memory)
            self._refresh_memory_info()
            self.log(f"Imported training CSV: {added} overrides, {notes} notes added.")
            messagebox.showinfo("Training Imported", f"Overrides added: {added}\nNotes added: {notes}")
        except Exception as exc:
            messagebox.showerror("Import Error", str(exc))

    def correct_last_scan_page(self) -> None:
        last_scan = self.storage.load_last_scan()
        if not last_scan:
            messagebox.showerror("No Scan Data", "No last_scan.json found. Run a scan first.")
            return
        CorrectionPicker(self, last_scan)

    def add_memory_note(self) -> None:
        note = simpledialog.askstring(
            "Add Memory Note",
            "Enter a rule/note to improve future overlap detection:",
            parent=self,
        )
        if not note:
            return
        note = note.strip()
        if not note:
            return
        notes = self.memory.setdefault("global_notes", [])
        if note not in notes:
            notes.append(note)
            self.storage.save_memory(self.memory)
            self._refresh_memory_info()
            self.log("Added memory note.")

    def save_memory(self) -> None:
        self.storage.save_memory(self.memory)
        self._refresh_memory_info()
        self.log("Memory saved.")
        messagebox.showinfo("Saved", "Memory saved.")

    def show_memory_stats(self) -> None:
        notes = len(self.memory.get("global_notes", []))
        overrides = len(self.memory.get("overrides", {}))
        corrections = len(self.memory.get("correction_history", []))
        messagebox.showinfo(
            "Memory Stats",
            f"Global notes: {notes}\nOverrides: {overrides}\nCorrections: {corrections}",
        )

    def pause_pipeline(self) -> None:
        if not self.worker_thread or not self.worker_thread.is_alive():
            return
        if self.pause_event.is_set():
            self.pause_event.clear()
            self.pause_btn_text.set("Pause")
            self._set_status("Resumed.")
            self.log("Pipeline resumed.")
        else:
            self.pause_event.set()
            self.pause_btn_text.set("Resume")
            self._set_status("Paused. Waiting after the current request.")
            self.log("Pipeline paused.")

    def stop_pipeline(self) -> None:
        self.cancel_event.set()
        self.pause_event.clear()
        if hasattr(self, "pause_btn_text"):
            self.pause_btn_text.set("Pause")
        self._set_status("Stop requested. Finishing the current request before shutdown.")
        self.log("Stop requested.")

    def run_pipeline(self) -> None:
        if self.worker_thread and self.worker_thread.is_alive():
            messagebox.showwarning("Busy", "A pipeline is already running.")
            return

        source_dir = Path(self.source_dir_var.get().strip())
        if not source_dir.exists() or not source_dir.is_dir():
            messagebox.showerror("Error", "Source directory does not exist.")
            return

        output_dir = Path(self.output_dir_var.get().strip()) if self.output_dir_var.get().strip() else source_dir
        output_dir.mkdir(parents=True, exist_ok=True)
        csv_path = output_dir / f"overlap_report_{now_file_ts()}.csv"
        self.csv_path_var.set(str(csv_path))

        custom_prompt = ""
        recursive = True
        live_output = True
        render_dpi = 220
        parameter_override = self._parameter_override()
        naming_meta = self._output_naming_meta()

        self.cancel_event.clear()
        self.pause_event.clear()
        self.pause_btn_text.set("Pause")
        self.progress["value"] = 0
        self._set_status("Starting detector...")

        def progress_cb(done: int, total: int) -> None:
            def _set() -> None:
                self.progress["maximum"] = total
                self.progress["value"] = done

            self.after(0, _set)

        def worker() -> None:
            live_csv_fh = None
            try:
                self.log("Detector started.")
                self.log(
                    "Settings: "
                    f"csv={self.act_csv_var.get()}, overlap={self.act_identify_var.get()}, "
                    f"extracted_original={self.act_extract_var.get()}, "
                    f"blurry={self.act_blurry_var.get()}, uncertain={self.act_uncertain_var.get()}"
                )
                src_pdfs = list_pdfs(source_dir, recursive=recursive)
                if not src_pdfs:
                    self._set_status("No source PDFs found.")
                    self.log("No PDF files found in source directory.")
                    return
                self._set_status(f"Scanning {len(src_pdfs)} source PDFs...")
                self.log(f"Source PDFs: {len(src_pdfs)}")
                engine = PythonHeuristicEngine(
                    memory=self.memory,
                    logger=self.log,
                    cancel_event=self.cancel_event,
                    pause_event=self.pause_event,
                    progress_cb=progress_cb,
                    render_dpi=render_dpi,
                    parameter_override=parameter_override,
                )

                live_csv_writer: Optional[csv.DictWriter] = None
                live_o_count = 0
                live_e_count = 0
                live_b_count = 0
                live_u_count = 0
                if live_output and self.act_csv_var.get():
                    csv_path.parent.mkdir(parents=True, exist_ok=True)
                    live_csv_fh = csv_path.open("w", newline="", encoding="utf-8")
                    live_csv_writer = csv.DictWriter(live_csv_fh, fieldnames=OVERLAP_CSV_FIELDS)
                    live_csv_writer.writeheader()
                    live_csv_fh.flush()
                    self.log(f"Live CSV started: {csv_path}")

                def on_source_page_result(rec: Dict[str, Any], pdf_path: Path, doc: fitz.Document) -> None:
                    nonlocal live_o_count, live_b_count, live_u_count
                    if rec.get("scope") != "source":
                        return
                    decision = str(rec.get("decision") or "unknown")
                    page_num = int(rec.get("page") or 0)
                    self._set_status(f"{pdf_path.name} p{page_num:03d}: {decision}")
                    if (
                        live_output
                        and self.act_csv_var.get()
                        and live_csv_writer
                        and live_csv_fh
                        and (rec.get("is_overlap") or rec.get("is_blurry") or rec.get("decision") == "uncertain")
                    ):
                        live_csv_writer.writerow(overlap_row_for_csv(rec))
                        live_csv_fh.flush()

                    if live_output and self.act_identify_var.get() and rec.get("is_overlap"):
                        if export_single_overlap_page_from_doc(
                            doc,
                            pdf_path,
                            int(rec["page"]),
                            self.log,
                            output_dir=output_dir,
                            naming_meta=naming_meta,
                        ):
                            live_o_count += 1

                    if live_output and self.act_blurry_var.get() and rec.get("is_blurry"):
                        if export_single_blurry_page_from_doc(
                            doc,
                            pdf_path,
                            int(rec["page"]),
                            self.log,
                            output_dir=output_dir,
                            naming_meta=naming_meta,
                        ):
                            live_b_count += 1

                    if live_output and self.act_uncertain_var.get() and rec.get("decision") == "uncertain":
                        if export_single_uncertain_page_from_doc(doc, pdf_path, int(rec["page"]), self.log, output_dir=output_dir):
                            live_u_count += 1

                def on_source_file_done(pdf_path: Path, doc: fitz.Document, file_records: List[Dict[str, Any]]) -> None:
                    nonlocal live_e_count
                    if self.cancel_event.is_set():
                        return
                    if live_output and self.act_extract_var.get():
                        if export_extracted_non_overlap_for_file(doc, pdf_path, file_records, self.log, output_dir=output_dir):
                            live_e_count += 1

                source_records = engine.scan_pdfs(
                    src_pdfs,
                    scope="source",
                    custom_prompt=custom_prompt,
                    on_page_result=on_source_page_result if live_output else None,
                    on_file_done=on_source_file_done if live_output else None,
                )
                all_records = list(source_records)

                self.storage.save_last_scan(all_records)
                self.log(f"Scan complete. Total pages processed: {len(all_records)}")
                source_rows = [r for r in source_records if r.get("scope") == "source"]
                overlap_count = sum(1 for r in source_rows if r.get("decision") == "overlap")
                blurry_count = sum(1 for r in source_rows if r.get("decision") == "blurry")
                clean_count = sum(1 for r in source_rows if r.get("decision") == "clean")
                uncertain_count = sum(1 for r in source_rows if r.get("decision") == "uncertain")
                total_source = len(source_rows)
                self.log(
                    "Decision summary: "
                    f"total={total_source}, overlap={overlap_count}, blurry={blurry_count}, "
                    f"clean={clean_count}, uncertain={uncertain_count}"
                )

                if self.cancel_event.is_set():
                    self._set_status("Stopped before finishing output actions.")
                    self.log("Pipeline stopped before actions.")
                    return

                if self.act_csv_var.get():
                    if live_output:
                        count = len(
                            [
                                r
                                for r in source_records
                                if r.get("is_overlap") or r.get("is_blurry") or r.get("decision") == "uncertain"
                            ]
                        )
                        self.log(
                            f"Action 1 done (live): CSV saved to {csv_path} (flagged rows={count})"
                        )
                    else:
                        row_count = write_source_csv(source_records, csv_path)
                        self.log(f"Action 1 done: CSV saved to {csv_path} (flagged rows={row_count})")

                if self.act_identify_var.get():
                    if live_output:
                        self.log(f"Action 2 done (live): exported Overlap pages = {live_o_count}")
                    else:
                        cnt = export_overlap_pages(source_records, self.log, output_dir=output_dir, naming_meta=naming_meta)
                        self.log(f"Action 2 done: exported Overlap pages = {cnt}")

                if self.act_extract_var.get():
                    if live_output:
                        self.log(f"Action 3 done (live): created Extracted Original files = {live_e_count}")
                    else:
                        cnt = export_extracted_non_overlap(source_records, self.log, output_dir=output_dir)
                        self.log(f"Action 3 done: created Extracted Original files = {cnt}")

                if self.act_blurry_var.get():
                    if live_output:
                        self.log(f"Action 4 done (live): exported Blurry pages = {live_b_count}")
                    else:
                        cnt = export_blurry_pages(source_records, self.log, output_dir=output_dir, naming_meta=naming_meta)
                        self.log(f"Action 4 done: exported Blurry pages = {cnt}")

                if self.act_uncertain_var.get():
                    if live_output:
                        self.log(f"Action 5 done (live): exported Uncertain pages = {live_u_count}")
                    else:
                        cnt = export_uncertain_pages(source_records, self.log, output_dir=output_dir)
                        self.log(f"Action 5 done: exported Uncertain pages = {cnt}")

                self._set_status(
                    f"Pipeline finished. overlap={overlap_count}, blurry={blurry_count}, clean={clean_count}, uncertain={uncertain_count}"
                )
                self.log("Pipeline finished.")
            except Exception:
                self._set_status("Pipeline crashed. Check the execution log.")
                self.log("Pipeline crashed:\n" + traceback.format_exc())
            finally:
                if live_csv_fh:
                    try:
                        live_csv_fh.close()
                    except Exception:
                        pass
                self.pause_event.clear()
                self.after(0, lambda: self.pause_btn_text.set("Pause"))
                self.after(0, lambda: self.progress.configure(value=0))

        self.worker_thread = threading.Thread(target=worker, daemon=True)
        self.worker_thread.start()


def main() -> None:
    app = App()
    app.mainloop()


if __name__ == "__main__":
    main()
