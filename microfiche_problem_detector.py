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
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import fitz  # PyMuPDF
import requests
import tkinter as tk
from PIL import Image, ImageOps
from tkinter import filedialog, font as tkfont, messagebox, ttk
from tkinter.scrolledtext import ScrolledText

APP_NAME = "Microfiche Problem Detector"
NOTEBOOK_HEIGHT_OVERLAP = 268
NOTEBOOK_HEIGHT_BLURRY = 360

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
PY_WIDTH_OVERLAP_REL_THRESHOLD = 1.03
BLURRY_LLM_PROMPT = (
    "Return strict JSON only with keys decision, is_blurry, confidence, reason. "
    "Choose decision from [blurry, clean, uncertain]. "
    "Read the page directly as an image snapshot. "
    "This is a transcript, so the important information is the student name, courses, and grades. "
    "Mark blurry only when the page information is too unclear to read: for example overexposed white pages, nearly all-white pages, or pages where the name, courses, and grades cannot be read at all. "
    "If any of that information is readable even partially, it is not blurry. "
    "Extract every page whose contents are unreadable. Use uncertain only for borderline cases."
)



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
        self.memory_path = self.root / "memory_store.json"
        self.last_scan_path = self.root / "last_scan.json"

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


def _remove_width_outliers(widths: List[float]) -> Tuple[List[float], int]:
    vals = [float(x) for x in widths if float(x) > 0]
    if len(vals) < 4:
        return vals, 0
    vals_sorted = sorted(vals)
    q1_idx = max(0, int((len(vals_sorted) - 1) * 0.25))
    q3_idx = max(0, int((len(vals_sorted) - 1) * 0.75))
    q1 = vals_sorted[q1_idx]
    q3 = vals_sorted[q3_idx]
    iqr = q3 - q1
    if iqr <= 0:
        return vals_sorted, 0
    low = q1 - 1.5 * iqr
    high = q3 + 1.5 * iqr
    kept = [v for v in vals_sorted if low <= v <= high]
    removed = len(vals_sorted) - len(kept)
    return kept or vals_sorted, removed


def estimate_pdf_width_sampled(
    pdf_path: Path,
    overlap_multiplier: float,
    dpi: int = 110,
    target_dpi: int = 220,
    max_width: int = 100000,
    quality: int = 40,
) -> Dict[str, Any]:
    widths: List[float] = []
    sampled_pages: List[int] = []
    doc = fitz.open(str(pdf_path))
    try:
        sample_indices = [i for i in range(len(doc)) if (i + 1) % 2 == 0]
        if not sample_indices:
            sample_indices = list(range(len(doc)))
        for idx in sample_indices:
            try:
                image_jpeg = render_page_jpeg(doc[idx], dpi=dpi, max_width=max_width, quality=quality)
                width = estimate_trimmed_body_width(image_jpeg)
                if width > 0:
                    scaled_width = width * (float(target_dpi) / float(max(dpi, 1)))
                    widths.append(scaled_width)
                    sampled_pages.append(idx + 1)
            except Exception:
                continue
    finally:
        doc.close()

    if not widths:
        return {
            "baseline_body_width": 0.0,
            "body_width_overlap_rel_threshold": round(float(overlap_multiplier or PY_WIDTH_OVERLAP_REL_THRESHOLD), 4),
            "body_width_overlap_threshold": 0.0,
            "sample_count": 0,
            "outliers_removed": 0,
            "sample_pages": "",
        }

    filtered, removed = _remove_width_outliers(widths)
    filtered_sorted = sorted(filtered)
    keep_count = max(1, int(round(len(filtered_sorted) * 0.85)))
    keep_vals = filtered_sorted[:keep_count]
    baseline = sum(keep_vals) / max(len(keep_vals), 1)
    rel = round(float(overlap_multiplier or PY_WIDTH_OVERLAP_REL_THRESHOLD), 4)
    return {
        "baseline_body_width": round(baseline, 3),
        "body_width_overlap_rel_threshold": rel,
        "body_width_overlap_threshold": round(baseline * rel, 3),
        "sample_count": len(widths),
        "outliers_removed": removed,
        "sample_pages": ",".join(str(p) for p in sampled_pages),
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



def classify_python_page(page_cues: Dict[str, Any], blurry_stats: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    trimmed_body_width = float(page_cues.get("trimmed_body_width") or 0.0)
    trimmed_body_rel_width = float(page_cues.get("trimmed_body_rel_width") or 0.0)
    trimmed_body_width_overlap_hint = bool(page_cues.get("trimmed_body_width_overlap_hint"))
    trimmed_body_width_overlap_threshold = float(page_cues.get("trimmed_body_width_overlap_threshold") or 0.0)
    trimmed_body_width_overlap_rel_threshold = float(page_cues.get("trimmed_body_width_overlap_rel_threshold") or PY_WIDTH_OVERLAP_REL_THRESHOLD)

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
    "run_ts",
    "relative_file",
    "page",
    "decision",
    "trimmed_body_width",
    "confidence",
    "reason",
    "status",
    "error_detail",
]

ESTIMATE_CSV_FIELDS = [
    "run_ts",
    "relative_file",
    "estimated_trimmed_width",
    "overlap_multiplier",
    "overlap_threshold",
    "sample_count",
    "outliers_removed",
    "sample_pages",
]


def overlap_row_for_csv(rec: Dict[str, Any]) -> Dict[str, Any]:
    row = dict(rec)
    row["signatures"] = " | ".join(rec.get("signatures", []))
    return {k: row.get(k, "") for k in OVERLAP_CSV_FIELDS}


def append_csv_rows(csv_path: Path, fieldnames: List[str], rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    exists = csv_path.exists()
    with csv_path.open("a", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=fieldnames)
        if not exists:
            w.writeheader()
        for row in rows:
            w.writerow({k: row.get(k, "") for k in fieldnames})
    return len(rows)


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



class OpenAICompatibleClient:
    def __init__(self, profile: ModelProfile):
        self.profile = profile
        alias = profile.model.strip() or profile.name.strip()
        self.model_candidates = [alias]
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

    def classify_blurry_page(self, image_jpeg: bytes, file_name: str, page_no: int, custom_prompt: str = "") -> Dict[str, Any]:
        b64 = base64.b64encode(image_jpeg).decode("ascii")
        prompt = (custom_prompt.strip() or BLURRY_LLM_PROMPT) + f" file={file_name} page={page_no}"
        errors: List[str] = []
        for model_id in self.model_candidates:
            ok, result = self._try_responses(model_id, prompt, b64, 80)
            if ok:
                return result
            errors.append(str(result.get("error", ""))[:220])
            if self.responses_only:
                continue
            ok, result = self._try_chat(model_id, prompt, b64, 80)
            if ok:
                return result
            errors.append(str(result.get("error", ""))[:220])
        return {
            "ok": False,
            "status": -1,
            "raw": "",
            "error": "Blurry classification failed. " + " || ".join(errors[:4]),
        }



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
        estimate_cb: Optional[Callable[[Path, Dict[str, Any]], None]] = None,
    ) -> None:
        self.memory = ensure_memory_schema(memory)
        self.log = logger
        self.cancel_event = cancel_event
        self.pause_event = pause_event
        self.progress_cb = progress_cb
        self.render_dpi = max(120, min(360, int(render_dpi)))
        self.parameter_override = parameter_override or {}
        self.estimate_cb = estimate_cb
        self.pdf_estimates: List[Dict[str, Any]] = []

    def memory_override(self, file_name: str, page_no: int) -> Optional[Dict[str, Any]]:
        key = f"{file_name.lower()}::{page_no}"
        return self.memory.get("overrides", {}).get(key)

    def _wait_if_paused(self) -> None:
        while self.pause_event and self.pause_event.is_set() and not self.cancel_event.is_set():
            time.sleep(0.15)

    def _apply_parameter_override(self, info: Dict[str, Any]) -> Dict[str, Any]:
        info = dict(info or {})
        overlap_multiplier = float(self.parameter_override.get("overlap_multiplier") or 0.0)
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
            pdf_width_info = self._apply_parameter_override(
                estimate_pdf_width_sampled(
                    pdf_path,
                    overlap_multiplier=float(self.parameter_override.get("overlap_multiplier") or PY_WIDTH_OVERLAP_REL_THRESHOLD),
                    dpi=110,
                    target_dpi=self.render_dpi,
                    max_width=100000,
                    quality=40,
                )
            )
            self.pdf_estimates.append(
                {
                    "relative_file": relative_batch_label(pdf_path),
                    "estimated_trimmed_width": pdf_width_info.get("baseline_body_width", 0.0),
                    "overlap_multiplier": pdf_width_info.get("body_width_overlap_rel_threshold", 0.0),
                    "overlap_threshold": pdf_width_info.get("body_width_overlap_threshold", 0.0),
                    "sample_count": pdf_width_info.get("sample_count", 0),
                    "outliers_removed": pdf_width_info.get("outliers_removed", 0),
                    "sample_pages": pdf_width_info.get("sample_pages", ""),
                }
            )
            if self.estimate_cb:
                try:
                    self.estimate_cb(pdf_path, pdf_width_info)
                except Exception:
                    pass
            self.log(
                f"Estimated width for {pdf_path.name}: "
                f"{pdf_width_info.get('baseline_body_width')} "
                f"(threshold={pdf_width_info.get('body_width_overlap_threshold')}, "
                f"multiplier={pdf_width_info.get('body_width_overlap_rel_threshold')}, "
                f"samples={pdf_width_info.get('sample_count')}, "
                f"outliers_removed={pdf_width_info.get('outliers_removed')})"
            )

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
                        "run_ts": now_ts(),
                        "relative_file": relative_batch_label(pdf_path),
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
                    page_cues = enrich_python_width_cues(page_cues, pdf_width_info)
                    py_result = classify_python_page(page_cues, None)
                    rec = {
                        "run_ts": now_ts(),
                        "relative_file": relative_batch_label(pdf_path),
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
                except Exception as exc:
                    self.log(f"Python heuristic failed {pdf_path} p{page_no}: {exc}")
                    rec = {
                        "run_ts": now_ts(),
                        "relative_file": relative_batch_label(pdf_path),
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


class BlurryLLMEngine:
    def __init__(
        self,
        client: OpenAICompatibleClient,
        logger,
        cancel_event: threading.Event,
        pause_event: Optional[threading.Event],
        progress_cb,
        render_dpi: int = 150,
        custom_prompt: str = "",
    ) -> None:
        self.client = client
        self.log = logger
        self.cancel_event = cancel_event
        self.pause_event = pause_event
        self.progress_cb = progress_cb
        self.render_dpi = max(110, min(220, int(render_dpi)))
        self.custom_prompt = custom_prompt.strip()

    def _wait_if_paused(self) -> None:
        while self.pause_event and self.pause_event.is_set() and not self.cancel_event.is_set():
            time.sleep(0.15)

    def scan_pdfs(
        self,
        pdf_paths: List[Path],
        scope: str,
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
        self.log(f"Blurry model: {self.client.profile.name}")

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
                try:
                    image_jpeg = render_page_jpeg(doc[idx], dpi=self.render_dpi, max_width=1400, quality=70)
                    result = self.client.classify_blurry_page(image_jpeg, file_name, page_no, custom_prompt=self.custom_prompt)
                    if not result.get("ok"):
                        raise RuntimeError(str(result.get("error", "unknown blurry error")))
                    obj = result.get("json", {})
                    decision, is_overlap, is_blurry = normalize_decision_fields(obj)
                    _ = is_overlap
                    rec = {
                        "run_ts": now_ts(),
                        "relative_file": relative_batch_label(pdf_path),
                        "source_directory": str(pdf_path.parent),
                        "file_name": file_name,
                        "file_path": str(pdf_path),
                        "page": page_no,
                        "decision": decision,
                        "is_overlap": False,
                        "is_blurry": bool(is_blurry and decision == "blurry"),
                        "confidence": float(obj.get("confidence", 0.0) or 0.0),
                        "overlap_type": "none",
                        "signatures": [],
                        "reason": str(obj.get("reason", ""))[:500],
                        "scope": scope,
                        "status": "ok",
                        "error_detail": "",
                        "trimmed_body_width": "",
                    }
                except Exception as exc:
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
                        "reason": f"blurry_llm_error: {exc}",
                        "scope": scope,
                        "status": "error",
                        "error_detail": f"blurry_llm_error: {exc}",
                        "trimmed_body_width": "",
                    }
                records.append(rec)
                file_records.append(rec)
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


def _clean_token(value: str) -> str:
    value = re.sub(r"[^A-Za-z0-9]+", "_", (value or "").strip())
    value = re.sub(r"_+", "_", value).strip("_")
    return value or "X"


def derive_path_tokens(src_path: Path) -> Dict[str, Any]:
    parts = list(src_path.resolve().parts)
    batch_idx = -1
    batch_num = ""
    for i, part in enumerate(parts[:-1]):
        m = re.fullmatch(r"Batch\s*(\d+)", part, flags=re.IGNORECASE)
        if m:
            batch_idx = i
            batch_num = m.group(1)
    if batch_idx >= 0:
        root = Path(*parts[: batch_idx + 1])
        tray_part = parts[batch_idx + 1] if len(parts) > batch_idx + 1 else ""
        tray_match = re.fullmatch(r"Tray\s*(\d+)", tray_part, flags=re.IGNORECASE)
        tray_num = tray_match.group(1) if tray_match else ""
        slot = parts[batch_idx + 2] if len(parts) > batch_idx + 2 else ""
        archive = parts[batch_idx + 3] if len(parts) > batch_idx + 3 else src_path.parent.name
        return {
            "root": root,
            "batch_token": f"B{batch_num}" if batch_num else _clean_token(parts[batch_idx]),
            "tray_token": f"T{tray_num}" if tray_num else _clean_token(tray_part),
            "slot_token": _clean_token(slot),
            "archive_token": _clean_token(archive),
        }
    return {
        "root": src_path.parent,
        "batch_token": "B",
        "tray_token": "T",
        "slot_token": _clean_token(src_path.parent.name),
        "archive_token": "A",
    }


def batch_root_for_path(path: Path) -> Path:
    resolved = path.resolve()
    parts = list(resolved.parts)
    for i, part in enumerate(parts):
        if re.fullmatch(r"Batch\s*\d+", part, flags=re.IGNORECASE):
            return Path(*parts[: i + 1])
    return resolved if resolved.is_dir() else resolved.parent


def relative_batch_label(src_path: Path) -> str:
    resolved = src_path.resolve()
    parts = list(resolved.parts)
    for i, part in enumerate(parts):
        batch_match = re.fullmatch(r"Batch\s*(\d+)", part, flags=re.IGNORECASE)
        if batch_match:
            rel_parts: List[str] = [f"Batch{batch_match.group(1)}"]
            for p in parts[i + 1 :]:
                tray_match = re.fullmatch(r"Tray\s*(\d+)", p, flags=re.IGNORECASE)
                if tray_match:
                    rel_parts.append(f"Tray{tray_match.group(1)}")
                else:
                    rel_parts.append(p)
            return "/".join(rel_parts)
    return resolved.name


def auto_output_root(src_path: Path, kind: str) -> Path:
    info = derive_path_tokens(src_path)
    root = Path(info["root"])
    return root / kind


def scan_output_root(source_dir: Path, kind: str) -> Path:
    batch_root = batch_root_for_path(source_dir)
    return batch_root.parent / kind if batch_root.name != source_dir.name or re.fullmatch(r"Batch\s*\d+", batch_root.name, flags=re.IGNORECASE) else source_dir / kind


def build_auto_output_name(prefix: str, src_path: Path, page: int) -> str:
    info = derive_path_tokens(src_path)
    if prefix == "U":
        return f"U_{src_path.stem}_P{page}.pdf"
    parts: List[str] = [str(info["batch_token"]), str(info["tray_token"]), str(info["slot_token"]), str(info["archive_token"])]
    if prefix == "B":
        parts.insert(0, "Blurry")
    parts.append(src_path.stem)
    parts.append(f"P{page}")
    return "_".join(parts) + ".pdf"


def export_single_tagged_page_from_doc(
    doc: fitz.Document,
    src_path: Path,
    page: int,
    prefix: str,
    logger,
) -> bool:
    kind = "Blurry" if prefix == "B" else ("Overlap" if prefix == "O" else "Uncertain")
    out_root = auto_output_root(src_path, kind)
    out_root.mkdir(parents=True, exist_ok=True)
    out = out_root / build_auto_output_name(prefix, src_path, page)
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
) -> bool:
    return export_single_tagged_page_from_doc(doc, src_path, page, "O", logger)


def export_single_blurry_page_from_doc(
    doc: fitz.Document,
    src_path: Path,
    page: int,
    logger,
) -> bool:
    return export_single_tagged_page_from_doc(doc, src_path, page, "B", logger)


def export_single_uncertain_page_from_doc(
    doc: fitz.Document, src_path: Path, page: int, logger
) -> bool:
    return export_single_tagged_page_from_doc(doc, src_path, page, "U", logger)


def export_extracted_overlap_removed_for_file(
    doc: fitz.Document,
    src_path: Path,
    file_records: List[Dict[str, Any]],
    logger,
) -> bool:
    marks: Dict[int, bool] = {}
    for r in file_records:
        if r.get("scope") != "source":
            continue
        marks[int(r["page"])] = bool(r.get("is_overlap"))
    if not any(marks.values()):
        return False

    keep_pages = [p for p, is_flagged in sorted(marks.items()) if not is_flagged]
    if not keep_pages:
        logger(f"No clean pages for {src_path.name}, skip EO_ output.")
        return False

    out = src_path.parent / f"EO_{src_path.name}"
    try:
        out_doc = fitz.open()
        for p in keep_pages:
            out_doc.insert_pdf(doc, from_page=p - 1, to_page=p - 1)
        out_doc.save(str(out))
        out_doc.close()
        return True
    except Exception as exc:
        logger(f"Create EO_ file failed {src_path.name}: {exc}")
        return False


def export_extracted_blurry_removed_for_file(
    doc: fitz.Document,
    src_path: Path,
    file_records: List[Dict[str, Any]],
    logger,
) -> bool:
    marks: Dict[int, bool] = {}
    for r in file_records:
        if r.get("scope") != "source":
            continue
        marks[int(r["page"])] = bool(r.get("is_blurry"))
    if not any(marks.values()):
        return False

    keep_pages = [p for p, is_flagged in sorted(marks.items()) if not is_flagged]
    if not keep_pages:
        logger(f"No clean pages for {src_path.name}, skip EB_ output.")
        return False

    out = src_path.parent / f"EB_{src_path.name}"
    try:
        out_doc = fitz.open()
        for p in keep_pages:
            out_doc.insert_pdf(doc, from_page=p - 1, to_page=p - 1)
        out_doc.save(str(out))
        out_doc.close()
        return True
    except Exception as exc:
        logger(f"Create EB_ file failed {src_path.name}: {exc}")
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



class App(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title(APP_NAME)
        self.geometry("520x820")
        self.minsize(480, 740)

        self.storage = Storage()
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
        root.columnconfigure(0, weight=1)
        root.rowconfigure(2, weight=1)

        self.csv_path_var = tk.StringVar()
        self.source_dir_var = tk.StringVar()
        self.param_overlap_multiplier_var = tk.StringVar(value="")
        self.estimated_normal_width_var = tk.StringVar(value="Estimated normal: -")
        self.ov_csv_var = tk.BooleanVar(value=True)
        self.ov_overlap_var = tk.BooleanVar(value=True)
        self.ov_eo_var = tk.BooleanVar(value=True)
        self.bl_csv_var = tk.BooleanVar(value=True)
        self.bl_blurry_var = tk.BooleanVar(value=True)
        self.bl_eb_var = tk.BooleanVar(value=True)

        self.mode_tabs = ttk.Notebook(root)
        self.mode_tabs.grid(row=0, column=0, sticky="nsew")

        overlap_tab = ttk.Frame(self.mode_tabs, padding=4)
        blurry_tab = ttk.Frame(self.mode_tabs, padding=4)
        self.mode_tabs.add(overlap_tab, text="Overlap")
        self.mode_tabs.add(blurry_tab, text="Blurry")
        self.mode_tabs.bind("<<NotebookTabChanged>>", self._on_mode_tab_changed)

        overlap_tab.columnconfigure(0, weight=1)
        blurry_tab.columnconfigure(0, weight=1)

        ov_source = ttk.LabelFrame(overlap_tab, text="Source")
        ov_source.grid(row=0, column=0, sticky="ew")
        ov_source.columnconfigure(1, weight=1)
        ttk.Label(ov_source, text="Scan Directory").grid(row=0, column=0, sticky="w", padx=8, pady=(8, 8))
        self.source_dir_entry = ttk.Entry(ov_source, textvariable=self.source_dir_var)
        self.source_dir_entry.grid(row=0, column=1, sticky="ew", padx=8, pady=(8, 8))
        ttk.Button(ov_source, text="Browse", command=self.pick_source_dir).grid(row=0, column=2, sticky="ew", padx=(0, 8), pady=(8, 8))

        ov_outputs = ttk.LabelFrame(overlap_tab, text="Outputs")
        ov_outputs.grid(row=1, column=0, sticky="ew", pady=(10, 0))
        ov_outputs.columnconfigure(0, weight=1)
        ov_outputs.columnconfigure(1, weight=1)
        self._make_check(ov_outputs, "CSV", self.ov_csv_var).grid(row=0, column=0, sticky="w", padx=8, pady=(8, 2))
        self._make_check(ov_outputs, "Overlap", self.ov_overlap_var).grid(row=0, column=1, sticky="w", padx=8, pady=(8, 2))
        self._make_check(ov_outputs, "Extracted Original", self.ov_eo_var).grid(row=1, column=0, sticky="w", padx=8, pady=(2, 8))

        param_box = ttk.LabelFrame(overlap_tab, text="Parameters")
        param_box.grid(row=2, column=0, sticky="ew", pady=(10, 0))
        param_box.columnconfigure(1, weight=1)
        ttk.Label(param_box, text="Overlap Multiplier").grid(row=0, column=0, sticky="w", padx=8, pady=(8, 8))
        self.param_overlap_entry = ttk.Entry(param_box, textvariable=self.param_overlap_multiplier_var, width=12)
        self.param_overlap_entry.grid(row=0, column=1, sticky="w", padx=(0, 8), pady=(8, 8))
        ttk.Label(param_box, textvariable=self.estimated_normal_width_var).grid(
            row=1, column=0, columnspan=2, sticky="w", padx=8, pady=(2, 6)
        )

        bl_source = ttk.LabelFrame(blurry_tab, text="Source")
        bl_source.grid(row=0, column=0, sticky="ew")
        bl_source.columnconfigure(1, weight=1)
        ttk.Label(bl_source, text="Scan Directory").grid(row=0, column=0, sticky="w", padx=8, pady=(8, 8))
        ttk.Entry(bl_source, textvariable=self.source_dir_var).grid(row=0, column=1, sticky="ew", padx=8, pady=(8, 8))
        ttk.Button(bl_source, text="Browse", command=self.pick_source_dir).grid(row=0, column=2, sticky="ew", padx=(0, 8), pady=(8, 8))

        bl_outputs = ttk.LabelFrame(blurry_tab, text="Outputs")
        bl_outputs.grid(row=1, column=0, sticky="ew", pady=(10, 0))
        bl_outputs.columnconfigure(0, weight=1)
        bl_outputs.columnconfigure(1, weight=1)
        self._make_check(bl_outputs, "CSV", self.bl_csv_var).grid(row=0, column=0, sticky="w", padx=8, pady=(8, 2))
        self._make_check(bl_outputs, "Blurry", self.bl_blurry_var).grid(row=0, column=1, sticky="w", padx=8, pady=(8, 2))
        self._make_check(bl_outputs, "Extracted Blurry Original", self.bl_eb_var).grid(row=1, column=0, sticky="w", padx=8, pady=(2, 8))

        bl_model = ttk.LabelFrame(blurry_tab, text="Model")
        bl_model.grid(row=2, column=0, sticky="ew", pady=(10, 0))
        ttk.Label(bl_model, text="GPT-5.4: Fast blurry-only check").grid(row=0, column=0, sticky="w", padx=8, pady=(8, 8))

        bl_prompt = ttk.LabelFrame(blurry_tab, text="Prompt")
        bl_prompt.grid(row=3, column=0, sticky="nsew", pady=(10, 0))
        bl_prompt.columnconfigure(0, weight=1)
        bl_prompt.rowconfigure(0, weight=1)
        self.blurry_prompt_text = ScrolledText(bl_prompt, height=3)
        self.blurry_prompt_text.grid(row=0, column=0, sticky="nsew", padx=8, pady=8)
        self._style_text_widget(self.blurry_prompt_text)

        run_box = ttk.LabelFrame(root, text="Run")
        run_box.grid(row=1, column=0, sticky="ew", pady=(10, 0))
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
        log_box.grid(row=2, column=0, sticky="nsew", pady=(10, 0))
        log_box.columnconfigure(0, weight=1)
        log_box.rowconfigure(0, weight=1)
        self.log_text = ScrolledText(log_box, height=18)
        self.log_text.grid(row=0, column=0, sticky="nsew", padx=8, pady=8)
        self._style_text_widget(self.log_text)

    def _load_defaults(self) -> None:
        default_dir = str(Path.cwd())
        self.source_dir_var.set(default_dir)
        self.csv_path_var.set("")
        self.param_overlap_multiplier_var.set(f"{PY_WIDTH_OVERLAP_REL_THRESHOLD:.2f}")
        self.estimated_normal_width_var.set("Current estimated width: -")
        self.blurry_prompt_text.delete("1.0", tk.END)
        self.blurry_prompt_text.insert("1.0", BLURRY_LLM_PROMPT)
        self._apply_mode_layout()

    def _parameter_override(self) -> Dict[str, float]:
        out: Dict[str, float] = {}
        try:
            overlap = float(self.param_overlap_multiplier_var.get().strip())
            if overlap > 0:
                out["overlap_multiplier"] = overlap
        except Exception:
            pass
        return out

    def _current_mode(self) -> str:
        current = self.mode_tabs.tab(self.mode_tabs.select(), "text").strip().lower()
        return "blurry" if current == "blurry" else "overlap"

    def _on_mode_tab_changed(self, _event: tk.Event) -> None:
        self._apply_mode_layout()

    def _apply_mode_layout(self) -> None:
        mode = self._current_mode()
        height = NOTEBOOK_HEIGHT_BLURRY if mode == "blurry" else NOTEBOOK_HEIGHT_OVERLAP
        self.mode_tabs.configure(height=height)

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
    def pick_source_dir(self) -> None:
        d = filedialog.askdirectory(title="Select Source Directory")
        if d:
            self.source_dir_var.set(d)
            self.csv_path_var.set("")
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

        mode = self._current_mode()
        recursive = True
        live_output = True
        parameter_override = self._parameter_override()
        blurry_prompt = self.blurry_prompt_text.get("1.0", tk.END).strip()
        batch_root = batch_root_for_path(source_dir)
        estimate_csv_path = batch_root / "estimated_widths.csv"
        problem_csv_path = batch_root / "problem_pages.csv"
        run_log_path = batch_root / f"{mode}_run_{now_file_ts()}.txt"
        self.csv_path_var.set(str(problem_csv_path))

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
            started_ts = now_ts()
            t0 = time.perf_counter()
            run_log_path.parent.mkdir(parents=True, exist_ok=True)
            run_log_fh = run_log_path.open("w", encoding="utf-8")

            def runlog(msg: str) -> None:
                stamp = f"[{now_ts()}] {msg}"
                self.log(msg)
                try:
                    run_log_fh.write(stamp + "\n")
                    run_log_fh.flush()
                except Exception:
                    pass

            try:
                runlog("Detector started.")
                src_pdfs = list_pdfs(source_dir, recursive=recursive)
                if not src_pdfs:
                    self._set_status("No source PDFs found.")
                    runlog("No PDF files found in source directory.")
                    return
                rel_files = [relative_batch_label(p) for p in src_pdfs]
                self._set_status(f"Scanning {len(src_pdfs)} source PDFs...")
                runlog(f"Mode: {mode}")
                runlog(f"Source directory: {source_dir}")
                runlog(f"Batch root: {batch_root}")
                runlog(f"File count: {len(src_pdfs)}")
                if rel_files:
                    runlog(f"File range: {rel_files[0]} -> {rel_files[-1]}")
                runlog(f"Multiplier: {parameter_override.get('overlap_multiplier', PY_WIDTH_OVERLAP_REL_THRESHOLD)}")
                runlog(f"Problem CSV: {problem_csv_path}")
                if mode == "overlap":
                    runlog(f"Estimated width CSV: {estimate_csv_path}")
                runlog(f"Run log: {run_log_path}")
                live_o_count = 0
                live_b_count = 0
                live_eo_count = 0
                live_eb_count = 0
                csv_enabled = self.bl_csv_var.get() if mode == "blurry" else self.ov_csv_var.get()

                def on_overlap_page_result(rec: Dict[str, Any], pdf_path: Path, doc: fitz.Document) -> None:
                    nonlocal live_o_count
                    if rec.get("scope") != "source":
                        return
                    decision = str(rec.get("decision") or "unknown")
                    page_num = int(rec.get("page") or 0)
                    self._set_status(f"{pdf_path.name} p{page_num:03d}: {decision}")
                    if live_output and self.ov_overlap_var.get() and rec.get("is_overlap"):
                        if export_single_overlap_page_from_doc(doc, pdf_path, int(rec["page"]), runlog):
                            live_o_count += 1

                def on_blurry_page_result(rec: Dict[str, Any], pdf_path: Path, doc: fitz.Document) -> None:
                    nonlocal live_b_count
                    if rec.get("scope") != "source":
                        return
                    decision = str(rec.get("decision") or "unknown")
                    page_num = int(rec.get("page") or 0)
                    self._set_status(f"{pdf_path.name} p{page_num:03d}: {decision}")
                    if live_output and self.bl_blurry_var.get() and rec.get("is_blurry"):
                        if export_single_blurry_page_from_doc(doc, pdf_path, int(rec["page"]), runlog):
                            live_b_count += 1

                def on_overlap_file_done(pdf_path: Path, doc: fitz.Document, file_records: List[Dict[str, Any]]) -> None:
                    nonlocal live_eo_count
                    if self.cancel_event.is_set():
                        return
                    if live_output and self.ov_eo_var.get():
                        if export_extracted_overlap_removed_for_file(doc, pdf_path, file_records, runlog):
                            live_eo_count += 1

                def on_blurry_file_done(pdf_path: Path, doc: fitz.Document, file_records: List[Dict[str, Any]]) -> None:
                    nonlocal live_eb_count
                    if self.cancel_event.is_set():
                        return
                    if live_output and self.bl_eb_var.get():
                        if export_extracted_blurry_removed_for_file(doc, pdf_path, file_records, runlog):
                            live_eb_count += 1

                if mode == "blurry":
                    profile = ModelProfile(
                        name="GPT-5.4",
                        base_url="https://api.xcode.best",
                        model="gpt-5.4",
                        api_key="sk-y685XcpUUiynjMHANJzAgWU5DUsYScWTrWnw9mRqCu0KzYcz",
                        timeout_sec=45,
                    )
                    engine = BlurryLLMEngine(
                        client=OpenAICompatibleClient(profile),
                        logger=runlog,
                        cancel_event=self.cancel_event,
                        pause_event=self.pause_event,
                        progress_cb=progress_cb,
                        render_dpi=150,
                        custom_prompt=blurry_prompt,
                    )
                    source_records = engine.scan_pdfs(
                        src_pdfs,
                        scope="source",
                        on_page_result=on_blurry_page_result if live_output else None,
                        on_file_done=on_blurry_file_done if live_output else None,
                    )
                else:
                    def estimate_cb(pdf_path: Path, info: Dict[str, Any]) -> None:
                        text = f"Current estimated width: {pdf_path.name} -> {float(info.get('baseline_body_width') or 0.0):.0f}"
                        self.after(0, lambda: self.estimated_normal_width_var.set(text))

                    engine = PythonHeuristicEngine(
                        memory=self.memory,
                        logger=runlog,
                        cancel_event=self.cancel_event,
                        pause_event=self.pause_event,
                        progress_cb=progress_cb,
                        render_dpi=220,
                        parameter_override=parameter_override,
                        estimate_cb=estimate_cb,
                    )
                    source_records = engine.scan_pdfs(
                        src_pdfs,
                        scope="source",
                        custom_prompt="",
                        on_page_result=on_overlap_page_result if live_output else None,
                        on_file_done=on_overlap_file_done if live_output else None,
                    )
                all_records = list(source_records)

                self.storage.save_last_scan(all_records)
                runlog(f"Scan complete. Total pages processed: {len(all_records)}")
                source_rows = [r for r in source_records if r.get("scope") == "source"]
                overlap_count = sum(1 for r in source_rows if r.get("decision") == "overlap")
                blurry_count = sum(1 for r in source_rows if r.get("decision") == "blurry")
                clean_count = sum(1 for r in source_rows if r.get("decision") == "clean")
                uncertain_count = sum(1 for r in source_rows if r.get("decision") == "uncertain")
                total_source = len(source_rows)
                runlog(
                    "Decision summary: "
                    f"total={total_source}, overlap={overlap_count}, blurry={blurry_count}, "
                    f"clean={clean_count}, uncertain={uncertain_count}"
                )

                if self.cancel_event.is_set():
                    self._set_status("Stopped before finishing output actions.")
                    runlog("Pipeline stopped before actions.")
                    return

                if csv_enabled:
                    problem_rows = [overlap_row_for_csv(r) for r in source_records if r.get("decision") != "clean"]
                    row_count = append_csv_rows(problem_csv_path, OVERLAP_CSV_FIELDS, problem_rows)
                    runlog(f"Problem rows appended: {row_count}")
                    if mode == "overlap":
                        estimate_rows = []
                        for row in getattr(engine, "pdf_estimates", []):
                            row = dict(row)
                            row["run_ts"] = started_ts
                            estimate_rows.append(row)
                        est_count = append_csv_rows(estimate_csv_path, ESTIMATE_CSV_FIELDS, estimate_rows)
                        runlog(f"Estimated width rows appended: {est_count}")

                if mode == "blurry":
                    if self.bl_blurry_var.get():
                        runlog(f"Exported Blurry pages: {live_b_count}")
                    if self.bl_eb_var.get():
                        runlog(f"Created Extracted Blurry Original files: {live_eb_count}")
                else:
                    if self.ov_overlap_var.get():
                        runlog(f"Exported Overlap pages: {live_o_count}")
                    if self.ov_eo_var.get():
                        runlog(f"Created Extracted Original files: {live_eo_count}")

                elapsed = time.perf_counter() - t0
                runlog(f"Elapsed seconds: {elapsed:.2f}")

                self._set_status(
                    f"Pipeline finished. overlap={overlap_count}, blurry={blurry_count}, clean={clean_count}, uncertain={uncertain_count}"
                )
                runlog("Pipeline finished.")
            except Exception:
                self._set_status("Pipeline crashed. Check the execution log.")
                runlog("Pipeline crashed:\n" + traceback.format_exc())
            finally:
                try:
                    run_log_fh.close()
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
