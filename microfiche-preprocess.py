#!/usr/bin/env python3
from __future__ import annotations

import ctypes
import csv
import datetime as dt
from dataclasses import dataclass
import io
import json
import os
import queue
import re
import shutil
import statistics
import threading
import sys
import time
import traceback
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import fitz  # PyMuPDF
import tkinter as tk
from PIL import Image, ImageOps
from tkinter import filedialog, font as tkfont, messagebox, ttk
from tkinter.scrolledtext import ScrolledText

APP_NAME = "Microfiche Problem Detector"
NOTEBOOK_HEIGHT_OVERLAP = 268
NOTEBOOK_HEIGHT_BLURRY = 360

UI_TOKENS: Dict[str, str] = {
    "bg": "#F9F9F9",
    "surface": "#FFFFFF",
    "surface_base": "#F9F9F9",
    "surface_section": "#EEEEEE",
    "surface_alt": "#F3F3F4",
    "surface_hover": "#E8E8E8",
    "border": "#D9D9D9",
    "border_soft": "#E7E7E7",
    "outline_strong": "#777777",
    "grid_line": "#E7E7E7",
    "text": "#0A0A0A",
    "text_soft": "#1A1C1C",
    "text_muted": "#6D6D6D",
    "chip_bg": "#FFFFFF",
    "chip_active_bg": "#111111",
    "chip_active_fg": "#FFFFFF",
    "button_bg": "#111111",
    "button_fg": "#FFFFFF",
    "button_secondary_bg": "#FFFFFF",
    "button_secondary_fg": "#111111",
    "button_secondary_border": "#111111",
    "input_bg": "#FFFFFF",
    "input_border": "#CFCFCF",
    "log_bg": "#FFFFFF",
    "progress_trough": "#DADADA",
    "progress_fill": "#111111",
}

BODY_DARK_THRESHOLD = 100
BODY_COVERAGE_FRAC = 0.30
PY_WIDTH_OVERLAP_REL_THRESHOLD = 1.03
DEFAULT_CROP_RATIO = 2.242
CROPPED_FILE_PREFIX = "CR_"



def now_ts() -> str:
    return dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def now_file_ts() -> str:
    return dt.datetime.now().strftime("%Y%m%d_%H%M%S")


def app_data_dir() -> Path:
    if os.name == "nt":
        base = Path(os.environ.get("APPDATA", str(Path.home())))
        root = base / "Microfiche-Preprocess"
    else:
        root = Path.home() / ".microfiche-preprocess"
    root.mkdir(parents=True, exist_ok=True)
    return root


def candidate_openai_sans_dirs() -> List[Path]:
    here = Path(__file__).resolve()
    candidates = [
        here.parent / "fonts",
        here.parent.parent / "fonts",
        Path("/Users/zjz/Documents/OpenAI Sans 2/OT"),
    ]
    unique: List[Path] = []
    seen: set[str] = set()
    for candidate in candidates:
        key = str(candidate)
        if key in seen:
            continue
        seen.add(key)
        unique.append(candidate)
    return unique


def register_openai_sans_fonts() -> None:
    if sys.platform != "darwin":
        return
    try:
        cf = ctypes.CDLL("/System/Library/Frameworks/CoreFoundation.framework/CoreFoundation")
        ct = ctypes.CDLL("/System/Library/Frameworks/CoreText.framework/CoreText")
    except Exception:
        return
    cf.CFURLCreateFromFileSystemRepresentation.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_long, ctypes.c_bool]
    cf.CFURLCreateFromFileSystemRepresentation.restype = ctypes.c_void_p
    cf.CFRelease.argtypes = [ctypes.c_void_p]
    ct.CTFontManagerRegisterFontsForURL.argtypes = [ctypes.c_void_p, ctypes.c_uint32, ctypes.POINTER(ctypes.c_void_p)]
    ct.CTFontManagerRegisterFontsForURL.restype = ctypes.c_bool
    for font_dir in candidate_openai_sans_dirs():
        if not font_dir.exists():
            continue
        for font_path in sorted(font_dir.glob("*.otf")):
            try:
                raw = str(font_path).encode()
                url = cf.CFURLCreateFromFileSystemRepresentation(None, raw, len(raw), False)
                if not url:
                    continue
                err = ctypes.c_void_p()
                ct.CTFontManagerRegisterFontsForURL(url, 1, ctypes.byref(err))
                cf.CFRelease(url)
            except Exception:
                continue

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


@dataclass
class PipelineController:
    cancel_event: threading.Event
    pause_event: threading.Event

    def wait_if_paused(self) -> None:
        while self.pause_event.is_set() and not self.cancel_event.is_set():
            time.sleep(0.15)


@dataclass
class PipelineHooks:
    log: Callable[[str], None]
    status: Callable[[str], None]
    progress: Callable[[int, int], None]
    overlap_estimate: Optional[Callable[[Path, Dict[str, Any]], None]] = None
    crop_detected: Optional[Callable[[Path, Dict[str, Any]], None]] = None
    replace_cropped_dir: Optional[Callable[[Path], None]] = None


@dataclass
class OverlapRunConfig:
    source_dir: Path
    batch_root: Path
    estimate_csv_path: Path
    problem_csv_path: Path
    run_log_path: Path
    parameter_override: Dict[str, float]
    recursive: bool = True
    export_csv: bool = True
    export_overlap_pages: bool = True
    export_extracted_original: bool = True
    render_dpi: int = 220


@dataclass
class CropRunConfig:
    source_dir: Path
    cropped_dir: Path
    uncropped_dir: Path
    crop_ratio: float
    run_log_path: Path
    render_dpi: int = 220


@dataclass
class ReplaceRunConfig:
    cropped_dir: Path
    replacement_dir: Path
    run_log_path: Path


@dataclass
class PdfToJpegRunConfig:
    source_dir: Path
    output_dir: Path
    run_log_path: Path
    render_dpi: int = 220
    quality: int = 92


@dataclass
class JpegToPdfRunConfig:
    source_dir: Path
    output_dir: Path
    run_log_path: Path


def list_pdfs(root: Path, recursive: bool) -> List[Path]:
    if recursive:
        return sorted([p for p in root.rglob("*.pdf") if p.is_file()])
    return sorted([p for p in root.glob("*.pdf") if p.is_file()])


def list_jpegs(root: Path, recursive: bool) -> List[Path]:
    patterns = ("*.jpg", "*.jpeg", "*.JPG", "*.JPEG")
    if recursive:
        out = [p for pattern in patterns for p in root.rglob(pattern) if p.is_file()]
    else:
        out = [p for pattern in patterns for p in root.glob(pattern) if p.is_file()]
    return sorted({p.resolve(): p for p in out}.values(), key=lambda path: str(path))


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
    parts.append(src_path.stem)
    parts.append(f"P{page}")
    return "_".join(parts) + ".pdf"


def detect_page_body_rect(
    page: fitz.Page,
    dpi: int = 220,
    dark_threshold: int = BODY_DARK_THRESHOLD,
    coverage_frac: float = BODY_COVERAGE_FRAC,
) -> Dict[str, Any]:
    image_jpeg = render_page_jpeg(page, dpi=dpi, max_width=100000)
    gray = Image.open(io.BytesIO(image_jpeg)).convert("L")
    bbox = compute_page_body_bbox(gray, dark_threshold=dark_threshold, coverage_frac=coverage_frac)
    if not bbox:
        raise ValueError("No page body detected after black-edge trimming.")

    img_w, img_h = gray.size
    page_rect = page.rect
    scale_x = page_rect.width / max(img_w, 1)
    scale_y = page_rect.height / max(img_h, 1)
    x0, y0, x1, y1 = bbox
    pdf_rect = fitz.Rect(
        page_rect.x0 + x0 * scale_x,
        page_rect.y0 + y0 * scale_y,
        page_rect.x0 + x1 * scale_x,
        page_rect.y0 + y1 * scale_y,
    )
    return {
        "image_bbox": bbox,
        "image_width": img_w,
        "image_height": img_h,
        "pdf_rect": pdf_rect,
        "body_width": float(pdf_rect.width),
        "body_height": float(pdf_rect.height),
        "body_ratio": float(pdf_rect.width) / max(float(pdf_rect.height), 1.0),
    }


def compute_left_anchored_crop_rect(body_rect: fitz.Rect, page_rect: fitz.Rect, crop_ratio: float) -> fitz.Rect:
    ratio = float(crop_ratio or 0.0)
    if ratio <= 0:
        raise ValueError("Crop ratio must be greater than zero.")
    if body_rect.width <= 0 or body_rect.height <= 0:
        raise ValueError("Body rectangle is empty.")

    target_width = float(body_rect.height) * ratio
    target_x1 = float(body_rect.x0) + target_width
    if target_x1 > float(page_rect.x1):
        raise ValueError(
            f"Crop rectangle exceeds the source page width for crop ratio {ratio:.3f}: "
            f"page_width={page_rect.width:.3f}, target_x1={target_x1:.3f}"
        )
    return fitz.Rect(body_rect.x0, body_rect.y0, target_x1, body_rect.y1)


def export_cropped_first_page(
    src_path: Path,
    out_path: Path,
    crop_ratio: float,
    logger,
    render_dpi: int = 220,
) -> Dict[str, Any]:
    try:
        doc = fitz.open(str(src_path))
    except Exception as exc:
        raise RuntimeError(f"Open failed: {exc}") from exc

    temp_path = out_path.with_name(f"{out_path.stem}.tmp-{now_file_ts()}{out_path.suffix}")
    try:
        if len(doc) <= 0:
            raise ValueError("Source PDF has no pages.")
        page = doc[0]
        body_info = detect_page_body_rect(page, dpi=render_dpi)
        crop_rect = compute_left_anchored_crop_rect(body_info["pdf_rect"], page.rect, crop_ratio)

        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_doc = fitz.open()
        try:
            out_page = out_doc.new_page(width=crop_rect.width, height=crop_rect.height)
            out_page.show_pdf_page(out_page.rect, doc, 0, clip=crop_rect)
            out_doc.save(str(temp_path), garbage=4, deflate=True)
        finally:
            out_doc.close()

        os.replace(str(temp_path), str(out_path))
        return {
            "body_width": round(body_info["body_width"], 3),
            "body_height": round(body_info["body_height"], 3),
            "body_ratio": round(body_info["body_ratio"], 4),
            "crop_width": round(float(crop_rect.width), 3),
            "crop_height": round(float(crop_rect.height), 3),
            "crop_ratio": round(float(crop_rect.width) / max(float(crop_rect.height), 1.0), 4),
        }
    except Exception:
        if temp_path.exists():
            try:
                temp_path.unlink()
            except Exception:
                logger(f"Failed to remove temporary crop file: {temp_path}")
        raise
    finally:
        doc.close()


def copy_source_to_uncropped(
    src_path: Path,
    out_path: Path,
    logger,
) -> Path:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = out_path.with_name(f"{out_path.stem}.tmp-{now_file_ts()}{out_path.suffix}")
    try:
        shutil.copy2(src_path, temp_path)
        os.replace(str(temp_path), str(out_path))
        return out_path
    except Exception:
        if temp_path.exists():
            try:
                temp_path.unlink()
            except Exception:
                logger(f"Failed to remove temporary uncropped file: {temp_path}")
        raise


def build_cropped_output_name(src_name: str) -> str:
    base = str(src_name or "").strip()
    if base.startswith(CROPPED_FILE_PREFIX):
        return base
    return f"{CROPPED_FILE_PREFIX}{base}"


def parse_tagged_source_pdf_path(tagged_pdf_path: Path) -> Dict[str, Any]:
    stem = tagged_pdf_path.stem
    if stem.startswith(CROPPED_FILE_PREFIX):
        stem = stem[len(CROPPED_FILE_PREFIX):]
    match = re.fullmatch(r"(.+)_P(\d+)", stem)
    if not match:
        raise ValueError(f"Tagged PDF name must end with _P<page>: {tagged_pdf_path.name}")

    prefix = match.group(1)
    page_no = int(match.group(2))
    parts = prefix.split("_")
    if len(parts) < 5:
        raise ValueError(f"Tagged PDF name does not contain enough path tokens: {tagged_pdf_path.name}")

    batch_token, tray_token, slot_token, archive_token = parts[:4]
    source_stem = "_".join(parts[4:]).strip()
    if not source_stem:
        raise ValueError(f"Tagged PDF name is missing the original source stem: {tagged_pdf_path.name}")

    return {
        "page": page_no,
        "batch_token": batch_token,
        "tray_token": tray_token,
        "slot_token": slot_token,
        "archive_token": archive_token,
        "source_stem": source_stem,
        "source_file_name": f"{source_stem}.pdf",
    }


def find_replacement_target(tagged_pdf_path: Path, replacement_dir: Path) -> Path:
    meta = parse_tagged_source_pdf_path(tagged_pdf_path)
    candidates = [p for p in replacement_dir.rglob(meta["source_file_name"]) if p.is_file()]
    if not candidates:
        raise FileNotFoundError(
            f"No original PDF named {meta['source_file_name']} was found under {replacement_dir}"
        )

    exact_matches: List[Path] = []
    for candidate in candidates:
        try:
            candidate_meta = derive_path_tokens(candidate)
        except Exception:
            continue
        if (
            str(candidate_meta.get("batch_token")) == meta["batch_token"]
            and str(candidate_meta.get("tray_token")) == meta["tray_token"]
            and str(candidate_meta.get("slot_token")) == meta["slot_token"]
            and str(candidate_meta.get("archive_token")) == meta["archive_token"]
        ):
            exact_matches.append(candidate)

    if len(exact_matches) == 1:
        return exact_matches[0]
    if len(exact_matches) > 1:
        raise RuntimeError(
            "Multiple original PDFs matched the same tagged cropped file: "
            + ", ".join(str(p) for p in exact_matches[:5])
        )
    if len(candidates) == 1:
        return candidates[0]
    raise RuntimeError(
        "Multiple candidate original PDFs were found but none matched the tagged path tokens exactly: "
        + ", ".join(str(p) for p in candidates[:5])
    )


def replace_pdf_page_with_single_page(
    original_pdf_path: Path,
    replacement_page_path: Path,
    page_no: int,
    logger,
) -> None:
    try:
        original_doc = fitz.open(str(original_pdf_path))
    except Exception as exc:
        raise RuntimeError(f"Open original failed: {exc}") from exc

    try:
        replacement_doc = fitz.open(str(replacement_page_path))
    except Exception as exc:
        original_doc.close()
        raise RuntimeError(f"Open replacement failed: {exc}") from exc

    temp_path = original_pdf_path.with_name(f"{original_pdf_path.stem}.tmp-{now_file_ts()}{original_pdf_path.suffix}")
    try:
        if len(replacement_doc) <= 0:
            raise ValueError("Replacement PDF has no pages.")
        if page_no <= 0 or page_no > len(original_doc):
            raise ValueError(
                f"Replacement page {page_no} is outside the original PDF page range 1..{len(original_doc)}"
            )

        out_doc = fitz.open()
        try:
            if page_no > 1:
                out_doc.insert_pdf(original_doc, from_page=0, to_page=page_no - 2)
            out_doc.insert_pdf(replacement_doc, from_page=0, to_page=0)
            if page_no < len(original_doc):
                out_doc.insert_pdf(original_doc, from_page=page_no, to_page=len(original_doc) - 1)
            out_doc.save(str(temp_path), garbage=4, deflate=True)
        finally:
            out_doc.close()

        os.replace(str(temp_path), str(original_pdf_path))
    except Exception:
        if temp_path.exists():
            try:
                temp_path.unlink()
            except Exception:
                logger(f"Failed to remove temporary replacement file: {temp_path}")
        raise
    finally:
        replacement_doc.close()
        original_doc.close()


def export_single_tagged_page_from_doc(
    doc: fitz.Document,
    src_path: Path,
    page: int,
    prefix: str,
    logger,
) -> bool:
    kind = "Overlap" if prefix == "O" else "Uncertain"
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
                if export_single_overlap_page_from_doc(doc, src, page, logger):
                    created += 1
            except Exception as exc:
                logger(f"Export overlap page failed {src.name} p{page}: {exc}")
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


def _open_run_log(run_log_path: Path, log_sink: Callable[[str], None]) -> Tuple[Any, Callable[[str], None]]:
    run_log_path.parent.mkdir(parents=True, exist_ok=True)
    run_log_fh = run_log_path.open("w", encoding="utf-8")

    def runlog(msg: str) -> None:
        stamp = f"[{now_ts()}] {msg}"
        log_sink(msg)
        try:
            run_log_fh.write(stamp + "\n")
            run_log_fh.flush()
        except Exception:
            pass

    return run_log_fh, runlog


def run_overlap_pipeline(
    config: OverlapRunConfig,
    hooks: PipelineHooks,
    controller: PipelineController,
    storage: Storage,
    memory: Dict[str, Any],
) -> Dict[str, Any]:
    started_ts = now_ts()
    t0 = time.perf_counter()
    run_log_fh, runlog = _open_run_log(config.run_log_path, hooks.log)
    try:
        runlog("Detector started.")
        src_pdfs = list_pdfs(config.source_dir, recursive=config.recursive)
        if not src_pdfs:
            hooks.status("No source PDFs found.")
            runlog("No PDF files found in source directory.")
            return {"ok": True, "mode": "overlap", "status": "empty"}

        rel_files = [relative_batch_label(p) for p in src_pdfs]
        hooks.status(f"Scanning {len(src_pdfs)} source PDFs...")
        runlog("Mode: overlap")
        runlog(f"Source directory: {config.source_dir}")
        runlog(f"Batch root: {config.batch_root}")
        runlog(f"File count: {len(src_pdfs)}")
        if rel_files:
            runlog(f"File range: {rel_files[0]} -> {rel_files[-1]}")
        runlog(f"Multiplier: {config.parameter_override.get('overlap_multiplier', PY_WIDTH_OVERLAP_REL_THRESHOLD)}")
        runlog(f"Problem CSV: {config.problem_csv_path}")
        runlog(f"Estimated width CSV: {config.estimate_csv_path}")
        runlog(f"Run log: {config.run_log_path}")
        live_overlap_count = 0
        live_extracted_original_count = 0

        def on_overlap_page_result(rec: Dict[str, Any], pdf_path: Path, doc: fitz.Document) -> None:
            nonlocal live_overlap_count
            if rec.get("scope") != "source":
                return
            decision = str(rec.get("decision") or "unknown")
            page_num = int(rec.get("page") or 0)
            hooks.status(f"{pdf_path.name} p{page_num:03d}: {decision}")
            if config.export_overlap_pages and rec.get("is_overlap"):
                if export_single_overlap_page_from_doc(doc, pdf_path, int(rec["page"]), runlog):
                    live_overlap_count += 1

        def on_overlap_file_done(pdf_path: Path, doc: fitz.Document, file_records: List[Dict[str, Any]]) -> None:
            nonlocal live_extracted_original_count
            if controller.cancel_event.is_set():
                return
            if config.export_extracted_original:
                if export_extracted_overlap_removed_for_file(doc, pdf_path, file_records, runlog):
                    live_extracted_original_count += 1

        def estimate_cb(pdf_path: Path, info: Dict[str, Any]) -> None:
            if hooks.overlap_estimate:
                hooks.overlap_estimate(pdf_path, info)

        engine = PythonHeuristicEngine(
            memory=memory,
            logger=runlog,
            cancel_event=controller.cancel_event,
            pause_event=controller.pause_event,
            progress_cb=hooks.progress,
            render_dpi=config.render_dpi,
            parameter_override=config.parameter_override,
            estimate_cb=estimate_cb,
        )
        source_records = engine.scan_pdfs(
            src_pdfs,
            scope="source",
            custom_prompt="",
            on_page_result=on_overlap_page_result,
            on_file_done=on_overlap_file_done,
        )
        if config.export_overlap_pages:
            runlog(f"Exported Overlap pages: {live_overlap_count}")
        if config.export_extracted_original:
            runlog(f"Created Extracted Original files: {live_extracted_original_count}")

        storage.save_last_scan(list(source_records))
        runlog(f"Scan complete. Total pages processed: {len(source_records)}")
        source_rows = [r for r in source_records if r.get("scope") == "source"]
        overlap_count = sum(1 for r in source_rows if r.get("decision") == "overlap")
        clean_count = sum(1 for r in source_rows if r.get("decision") == "clean")
        uncertain_count = sum(1 for r in source_rows if r.get("decision") == "uncertain")
        total_source = len(source_rows)
        runlog(
            "Decision summary: "
            f"total={total_source}, overlap={overlap_count}, "
            f"clean={clean_count}, uncertain={uncertain_count}"
        )

        if controller.cancel_event.is_set():
            hooks.status("Stopped before finishing output actions.")
            runlog("Pipeline stopped before actions.")
            return {
                "ok": True,
                "mode": "overlap",
                "cancelled": True,
                "overlap_count": overlap_count,
                "clean_count": clean_count,
                "uncertain_count": uncertain_count,
            }

        if config.export_csv:
            problem_rows = [overlap_row_for_csv(r) for r in source_records if r.get("decision") != "clean"]
            row_count = append_csv_rows(config.problem_csv_path, OVERLAP_CSV_FIELDS, problem_rows)
            runlog(f"Problem rows appended: {row_count}")
            estimate_rows = []
            for row in getattr(engine, "pdf_estimates", []):
                estimate_row = dict(row)
                estimate_row["run_ts"] = started_ts
                estimate_rows.append(estimate_row)
            est_count = append_csv_rows(config.estimate_csv_path, ESTIMATE_CSV_FIELDS, estimate_rows)
            runlog(f"Estimated width rows appended: {est_count}")

        elapsed = time.perf_counter() - t0
        runlog(f"Elapsed seconds: {elapsed:.2f}")
        hooks.status(
            f"Pipeline finished. overlap={overlap_count}, clean={clean_count}, uncertain={uncertain_count}"
        )
        runlog("Pipeline finished.")
        return {
            "ok": True,
            "mode": "overlap",
            "overlap_count": overlap_count,
            "clean_count": clean_count,
            "uncertain_count": uncertain_count,
            "elapsed_seconds": round(elapsed, 2),
            "run_log_path": str(config.run_log_path),
        }
    except Exception:
        hooks.status("Pipeline crashed. Check the execution log.")
        runlog("Pipeline crashed:\n" + traceback.format_exc())
        return {
            "ok": False,
            "mode": "overlap",
            "run_log_path": str(config.run_log_path),
        }
    finally:
        try:
            run_log_fh.close()
        except Exception:
            pass


def run_crop_pipeline(
    config: CropRunConfig,
    hooks: PipelineHooks,
    controller: PipelineController,
) -> Dict[str, Any]:
    t0 = time.perf_counter()
    run_log_fh, runlog = _open_run_log(config.run_log_path, hooks.log)
    try:
        runlog("Detector started.")
        src_pdfs = list_pdfs(config.source_dir, recursive=False)
        if not src_pdfs:
            hooks.status("No crop PDFs found.")
            runlog("No PDF files found in crop directory.")
            return {"ok": True, "mode": "crop", "status": "empty"}

        hooks.status(f"Cropping {len(src_pdfs)} PDFs...")
        runlog("Mode: crop")
        runlog(f"Crop directory: {config.source_dir}")
        runlog(f"Cropped directory: {config.cropped_dir}")
        runlog(f"Uncropped directory: {config.uncropped_dir}")
        runlog(f"Crop ratio: {config.crop_ratio}")
        runlog(f"Run log: {config.run_log_path}")
        hooks.progress(0, max(len(src_pdfs), 1))
        created_count = 0
        uncropped_count = 0
        error_count = 0

        for index, src_pdf in enumerate(src_pdfs, start=1):
            controller.wait_if_paused()
            if controller.cancel_event.is_set():
                runlog("Crop stopped before completion.")
                break

            hooks.status(f"Cropping {src_pdf.name}")
            out_pdf = config.cropped_dir / build_cropped_output_name(src_pdf.name)
            uncropped_pdf = config.uncropped_dir / src_pdf.name
            try:
                info = export_cropped_first_page(
                    src_pdf,
                    out_pdf,
                    crop_ratio=config.crop_ratio,
                    logger=runlog,
                    render_dpi=config.render_dpi,
                )
                created_count += 1
                if uncropped_pdf.exists():
                    uncropped_pdf.unlink()
                if hooks.crop_detected:
                    hooks.crop_detected(src_pdf, info)
                if hooks.replace_cropped_dir:
                    hooks.replace_cropped_dir(config.cropped_dir)
                runlog(
                    f"Cropped {src_pdf.name} -> {out_pdf} "
                    f"(body_ratio={info['body_ratio']}, crop_ratio={info['crop_ratio']})"
                )
            except Exception as exc:
                try:
                    if out_pdf.exists():
                        out_pdf.unlink()
                    copied_path = copy_source_to_uncropped(src_pdf, uncropped_pdf, runlog)
                    uncropped_count += 1
                    runlog(f"Uncropped {src_pdf.name} -> {copied_path} reason={exc}")
                except Exception as copy_exc:
                    error_count += 1
                    runlog(f"Crop failed {src_pdf.name}: {exc}")
                    runlog(f"Uncropped copy failed {src_pdf.name}: {copy_exc}")
            hooks.progress(index, max(len(src_pdfs), 1))

        elapsed = time.perf_counter() - t0
        runlog(f"Cropped files: {created_count}")
        runlog(f"Uncropped files: {uncropped_count}")
        runlog(f"Crop errors: {error_count}")
        runlog(f"Elapsed seconds: {elapsed:.2f}")
        if controller.cancel_event.is_set():
            hooks.status(
                f"Crop stopped. cropped={created_count}, uncropped={uncropped_count}, errors={error_count}"
            )
            cancelled = True
        else:
            hooks.status(
                f"Crop finished. cropped={created_count}, uncropped={uncropped_count}, errors={error_count}"
            )
            runlog("Pipeline finished.")
            cancelled = False
        return {
            "ok": True,
            "mode": "crop",
            "cancelled": cancelled,
            "cropped_count": created_count,
            "uncropped_count": uncropped_count,
            "error_count": error_count,
            "elapsed_seconds": round(elapsed, 2),
            "run_log_path": str(config.run_log_path),
        }
    except Exception:
        hooks.status("Pipeline crashed. Check the execution log.")
        runlog("Pipeline crashed:\n" + traceback.format_exc())
        return {
            "ok": False,
            "mode": "crop",
            "run_log_path": str(config.run_log_path),
        }
    finally:
        try:
            run_log_fh.close()
        except Exception:
            pass


def run_replace_pipeline(
    config: ReplaceRunConfig,
    hooks: PipelineHooks,
    controller: PipelineController,
) -> Dict[str, Any]:
    t0 = time.perf_counter()
    run_log_fh, runlog = _open_run_log(config.run_log_path, hooks.log)
    try:
        runlog("Detector started.")
        src_pdfs = list_pdfs(config.cropped_dir, recursive=False)
        if not src_pdfs:
            hooks.status("No cropped PDFs found.")
            runlog("No PDF files found in cropped directory.")
            return {"ok": True, "mode": "replace", "status": "empty"}

        hooks.status(f"Replacing pages from {len(src_pdfs)} cropped PDFs...")
        runlog("Mode: replace")
        runlog(f"Cropped directory: {config.cropped_dir}")
        runlog(f"Replacement directory: {config.replacement_dir}")
        runlog(f"Run log: {config.run_log_path}")
        hooks.progress(0, max(len(src_pdfs), 1))
        replaced_count = 0
        error_count = 0

        for index, cropped_pdf in enumerate(src_pdfs, start=1):
            controller.wait_if_paused()
            if controller.cancel_event.is_set():
                runlog("Replace stopped before completion.")
                break

            hooks.status(f"Replacing {cropped_pdf.name}")
            try:
                meta = parse_tagged_source_pdf_path(cropped_pdf)
                target_pdf = find_replacement_target(cropped_pdf, config.replacement_dir)
                replace_pdf_page_with_single_page(
                    target_pdf,
                    cropped_pdf,
                    page_no=int(meta["page"]),
                    logger=runlog,
                )
                replaced_count += 1
                runlog(
                    f"Replaced {target_pdf} page {meta['page']} "
                    f"from {cropped_pdf.name}"
                )
            except Exception as exc:
                error_count += 1
                runlog(f"Replace failed {cropped_pdf.name}: {exc}")
            hooks.progress(index, max(len(src_pdfs), 1))

        elapsed = time.perf_counter() - t0
        runlog(f"Replaced pages: {replaced_count}")
        runlog(f"Replace errors: {error_count}")
        runlog(f"Elapsed seconds: {elapsed:.2f}")
        if controller.cancel_event.is_set():
            hooks.status(f"Replace stopped. replaced={replaced_count}, errors={error_count}")
            cancelled = True
        else:
            hooks.status(f"Replace finished. replaced={replaced_count}, errors={error_count}")
            runlog("Pipeline finished.")
            cancelled = False
        return {
            "ok": True,
            "mode": "replace",
            "cancelled": cancelled,
            "replaced_count": replaced_count,
            "error_count": error_count,
            "elapsed_seconds": round(elapsed, 2),
            "run_log_path": str(config.run_log_path),
        }
    except Exception:
        hooks.status("Pipeline crashed. Check the execution log.")
        runlog("Pipeline crashed:\n" + traceback.format_exc())
        return {
            "ok": False,
            "mode": "replace",
            "run_log_path": str(config.run_log_path),
        }
    finally:
        try:
            run_log_fh.close()
        except Exception:
            pass


def run_pdf_to_jpeg_pipeline(
    config: PdfToJpegRunConfig,
    hooks: PipelineHooks,
    controller: PipelineController,
) -> Dict[str, Any]:
    t0 = time.perf_counter()
    run_log_fh, runlog = _open_run_log(config.run_log_path, hooks.log)
    try:
        runlog("Detector started.")
        src_pdfs = list_pdfs(config.source_dir, recursive=False)
        if not src_pdfs:
            hooks.status("No PDFs found.")
            runlog("No PDF files found in source directory.")
            return {"ok": True, "mode": "pdf-to-jpeg", "status": "empty"}

        hooks.status(f"Converting {len(src_pdfs)} PDFs...")
        runlog("Mode: pdf-to-jpeg")
        runlog(f"Source directory: {config.source_dir}")
        runlog(f"Output directory: {config.output_dir}")
        runlog(f"Run log: {config.run_log_path}")
        hooks.progress(0, max(len(src_pdfs), 1))
        created_count = 0
        error_count = 0

        for index, src_pdf in enumerate(src_pdfs, start=1):
            controller.wait_if_paused()
            if controller.cancel_event.is_set():
                runlog("PDF to JPEG stopped before completion.")
                break

            hooks.status(f"Converting {src_pdf.name}")
            pdf_output_dir = config.output_dir / src_pdf.stem
            try:
                doc = fitz.open(str(src_pdf))
            except Exception as exc:
                error_count += 1
                runlog(f"Open failed {src_pdf.name}: {exc}")
                hooks.progress(index, max(len(src_pdfs), 1))
                continue

            try:
                pdf_output_dir.mkdir(parents=True, exist_ok=True)
                file_created = 0
                for page_index in range(len(doc)):
                    page = doc[page_index]
                    image_bytes = render_page_jpeg(
                        page,
                        dpi=config.render_dpi,
                        max_width=None,
                        quality=config.quality,
                    )
                    out_name = f"{src_pdf.stem}-p{page_index + 1:04d}.jpg"
                    out_path = pdf_output_dir / out_name
                    temp_path = out_path.with_name(f"{out_path.stem}.tmp-{now_file_ts()}{out_path.suffix}")
                    try:
                        temp_path.write_bytes(image_bytes)
                        os.replace(str(temp_path), str(out_path))
                        file_created += 1
                    except Exception:
                        if temp_path.exists():
                            try:
                                temp_path.unlink()
                            except Exception:
                                runlog(f"Failed to remove temporary JPEG file: {temp_path}")
                        raise
                created_count += file_created
                runlog(f"Converted {src_pdf.name} -> {pdf_output_dir} pages={file_created}")
            except Exception as exc:
                error_count += 1
                runlog(f"PDF to JPEG failed {src_pdf.name}: {exc}")
            finally:
                doc.close()
            hooks.progress(index, max(len(src_pdfs), 1))

        elapsed = time.perf_counter() - t0
        runlog(f"JPEG files created: {created_count}")
        runlog(f"PDF to JPEG errors: {error_count}")
        runlog(f"Elapsed seconds: {elapsed:.2f}")
        if controller.cancel_event.is_set():
            hooks.status(f"PDF to JPEG stopped. created={created_count}, errors={error_count}")
            cancelled = True
        else:
            hooks.status(f"PDF to JPEG finished. created={created_count}, errors={error_count}")
            runlog("Pipeline finished.")
            cancelled = False
        return {
            "ok": True,
            "mode": "pdf-to-jpeg",
            "cancelled": cancelled,
            "created_count": created_count,
            "error_count": error_count,
            "elapsed_seconds": round(elapsed, 2),
            "run_log_path": str(config.run_log_path),
        }
    except Exception:
        hooks.status("Pipeline crashed. Check the execution log.")
        runlog("Pipeline crashed:\n" + traceback.format_exc())
        return {
            "ok": False,
            "mode": "pdf-to-jpeg",
            "run_log_path": str(config.run_log_path),
        }
    finally:
        try:
            run_log_fh.close()
        except Exception:
            pass


def run_jpeg_to_pdf_pipeline(
    config: JpegToPdfRunConfig,
    hooks: PipelineHooks,
    controller: PipelineController,
) -> Dict[str, Any]:
    t0 = time.perf_counter()
    run_log_fh, runlog = _open_run_log(config.run_log_path, hooks.log)
    try:
        runlog("Detector started.")
        src_images = list_jpegs(config.source_dir, recursive=False)
        if not src_images:
            hooks.status("No JPEGs found.")
            runlog("No JPEG files found in source directory.")
            return {"ok": True, "mode": "jpeg-to-pdf", "status": "empty"}

        hooks.status(f"Converting {len(src_images)} JPEGs...")
        runlog("Mode: jpeg-to-pdf")
        runlog(f"Source directory: {config.source_dir}")
        runlog(f"Output directory: {config.output_dir}")
        runlog(f"Run log: {config.run_log_path}")
        hooks.progress(0, max(len(src_images), 1))
        created_count = 0
        error_count = 0
        config.output_dir.mkdir(parents=True, exist_ok=True)

        for index, src_image in enumerate(src_images, start=1):
            controller.wait_if_paused()
            if controller.cancel_event.is_set():
                runlog("JPEG to PDF stopped before completion.")
                break

            hooks.status(f"Converting {src_image.name}")
            out_path = config.output_dir / f"{src_image.stem}.pdf"
            temp_path = out_path.with_name(f"{out_path.stem}.tmp-{now_file_ts()}{out_path.suffix}")
            try:
                raw_img = Image.open(src_image)
                img: Optional[Image.Image] = None
                try:
                    img = ImageOps.exif_transpose(raw_img).convert("RGB")
                    img_bytes = io.BytesIO()
                    img.save(img_bytes, format="JPEG", quality=95, optimize=True)
                    width = max(int(img.width), 1)
                    height = max(int(img.height), 1)
                finally:
                    raw_img.close()
                    if img is not None:
                        img.close()

                doc = fitz.open()
                try:
                    page = doc.new_page(width=float(width), height=float(height))
                    page.insert_image(page.rect, stream=img_bytes.getvalue())
                    doc.save(str(temp_path), garbage=4, deflate=True)
                finally:
                    doc.close()
                os.replace(str(temp_path), str(out_path))
                created_count += 1
                runlog(f"Converted {src_image.name} -> {out_path.name}")
            except Exception as exc:
                error_count += 1
                runlog(f"JPEG to PDF failed {src_image.name}: {exc}")
                if temp_path.exists():
                    try:
                        temp_path.unlink()
                    except Exception:
                        runlog(f"Failed to remove temporary PDF file: {temp_path}")
            hooks.progress(index, max(len(src_images), 1))

        elapsed = time.perf_counter() - t0
        runlog(f"PDF files created: {created_count}")
        runlog(f"JPEG to PDF errors: {error_count}")
        runlog(f"Elapsed seconds: {elapsed:.2f}")
        if controller.cancel_event.is_set():
            hooks.status(f"JPEG to PDF stopped. created={created_count}, errors={error_count}")
            cancelled = True
        else:
            hooks.status(f"JPEG to PDF finished. created={created_count}, errors={error_count}")
            runlog("Pipeline finished.")
            cancelled = False
        return {
            "ok": True,
            "mode": "jpeg-to-pdf",
            "cancelled": cancelled,
            "created_count": created_count,
            "error_count": error_count,
            "elapsed_seconds": round(elapsed, 2),
            "run_log_path": str(config.run_log_path),
        }
    except Exception:
        hooks.status("Pipeline crashed. Check the execution log.")
        runlog("Pipeline crashed:\n" + traceback.format_exc())
        return {
            "ok": False,
            "mode": "jpeg-to-pdf",
            "run_log_path": str(config.run_log_path),
        }
    finally:
        try:
            run_log_fh.close()
        except Exception:
            pass



class PillButton(tk.Canvas):
    def __init__(
        self,
        parent: tk.Widget,
        *,
        text: str,
        command: Optional[Callable[[], None]],
        font: tkfont.Font,
        palette_off: Dict[str, str],
        palette_on: Optional[Dict[str, str]] = None,
        height: int = 32,
        radius: int = 16,
        padding_x: int = 16,
        text_offset_y: int = -1,
        min_width: int = 44,
        stretch: bool = False,
        align: str = "center",
    ) -> None:
        self.font_ref = font
        self.text_value = text
        self.command = command
        self.palette_off = dict(palette_off)
        self.palette_on = dict(palette_on or palette_off)
        self.height_value = height
        self.radius_value = radius
        self.padding_x = padding_x
        self.text_offset_y = text_offset_y
        self.min_width = max(1, int(min_width))
        self.stretch = stretch
        self.align = align
        self.active = False
        self.hovered = False
        width = max(self.min_width, font.measure(text) + padding_x * 2)
        super().__init__(
            parent,
            width=width,
            height=height,
            highlightthickness=0,
            bd=0,
            bg=str(parent.cget("background")),
            relief="flat",
            cursor="hand2",
        )
        self.bind("<Enter>", self._on_enter)
        self.bind("<Leave>", self._on_leave)
        self.bind("<Button-1>", self._on_click)
        self.bind("<Configure>", self._on_configure)
        self._redraw()

    def _draw_round_rect(self, x1: int, y1: int, x2: int, y2: int, radius: int, **kwargs: Any) -> int:
        points = [
            x1 + radius, y1,
            x1 + radius, y1,
            x2 - radius, y1,
            x2 - radius, y1,
            x2, y1,
            x2, y1 + radius,
            x2, y1 + radius,
            x2, y2 - radius,
            x2, y2 - radius,
            x2, y2,
            x2 - radius, y2,
            x2 - radius, y2,
            x1 + radius, y2,
            x1 + radius, y2,
            x1, y2,
            x1, y2 - radius,
            x1, y2 - radius,
            x1, y1 + radius,
            x1, y1 + radius,
            x1, y1,
        ]
        return self.create_polygon(points, smooth=True, splinesteps=36, **kwargs)

    def _current_palette(self) -> Dict[str, str]:
        palette = self.palette_on if self.active else self.palette_off
        out = dict(palette)
        if self.hovered and not self.active:
            out["fill"] = palette.get("hover_fill", palette["fill"])
            out["text"] = palette.get("hover_text", palette["text"])
            out["outline"] = palette.get("hover_outline", palette["outline"])
        return out

    def _redraw(self) -> None:
        requested_width = max(self.min_width, self.font_ref.measure(self.text_value) + self.padding_x * 2)
        width = max(requested_width, self.winfo_width()) if self.stretch else requested_width
        if not self.stretch:
            self.configure(width=width, height=self.height_value)
        else:
            self.configure(height=self.height_value)
        self.delete("all")
        palette = self._current_palette()
        self._draw_round_rect(
            1,
            1,
            width - 1,
            self.height_value - 1,
            self.radius_value,
            fill=palette["fill"],
            outline=palette["outline"],
            width=1,
        )
        text_x = width / 2
        anchor = "center"
        if self.align == "left":
            text_x = self.padding_x
            anchor = "w"
        self.create_text(
            text_x,
            self.height_value / 2 + self.text_offset_y,
            text=self.text_value,
            fill=palette["text"],
            font=self.font_ref,
            anchor=anchor,
        )

    def _on_enter(self, _event: tk.Event) -> None:
        self.hovered = True
        self._redraw()

    def _on_leave(self, _event: tk.Event) -> None:
        self.hovered = False
        self._redraw()

    def _on_click(self, _event: tk.Event) -> None:
        if self.command:
            self.command()

    def _on_configure(self, _event: tk.Event) -> None:
        if self.stretch:
            self._redraw()

    def set_active(self, active: bool) -> None:
        self.active = active
        self._redraw()

    def set_text(self, text: str) -> None:
        self.text_value = text
        self._redraw()




class RoundedSurface(tk.Canvas):
    def __init__(
        self,
        parent: tk.Widget,
        *,
        fill: str,
        outline: str,
        radius: int = 24,
        pad_x: int = 14,
        pad_y: int = 12,
    ) -> None:
        super().__init__(
            parent,
            highlightthickness=0,
            bd=0,
            relief="flat",
            bg=str(parent.cget("background")),
            height=20,
        )
        self.fill_color = fill
        self.outline_color = outline
        self.radius_value = radius
        self.pad_x = pad_x
        self.pad_y = pad_y
        self.content = tk.Frame(self, bg=fill)
        self.window = self.create_window(pad_x, pad_y, anchor="nw", window=self.content)
        self.bind("<Configure>", self._on_canvas_configure)
        self.content.bind("<Configure>", self._on_content_configure)

    def _draw_round_rect(self, x1: int, y1: int, x2: int, y2: int, radius: int, **kwargs: Any) -> int:
        points = [
            x1 + radius, y1,
            x1 + radius, y1,
            x2 - radius, y1,
            x2 - radius, y1,
            x2, y1,
            x2, y1 + radius,
            x2, y1 + radius,
            x2, y2 - radius,
            x2, y2 - radius,
            x2, y2,
            x2 - radius, y2,
            x2 - radius, y2,
            x1 + radius, y2,
            x1 + radius, y2,
            x1, y2,
            x1, y2 - radius,
            x1, y2 - radius,
            x1, y1 + radius,
            x1, y1 + radius,
            x1, y1,
        ]
        return self.create_polygon(points, smooth=True, splinesteps=36, **kwargs)

    def _on_canvas_configure(self, event: tk.Event) -> None:
        self.itemconfigure(self.window, width=max(1, int(event.width) - self.pad_x * 2))
        self._redraw()

    def _on_content_configure(self, _event: tk.Event) -> None:
        target_height = self.content.winfo_reqheight() + self.pad_y * 2
        if int(float(self.cget("height"))) != target_height:
            self.configure(height=target_height)
        self.after_idle(self._redraw)

    def _redraw(self) -> None:
        width = max(2, self.winfo_width())
        height = max(2, self.winfo_height())
        self.delete("surface")
        self._draw_round_rect(
            1,
            1,
            width - 1,
            height - 1,
            self.radius_value,
            fill=self.fill_color,
            outline=self.outline_color,
            width=1,
            tags="surface",
        )
        self.tag_lower("surface")


class App(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.ui = dict(UI_TOKENS)
        self.title(APP_NAME)
        self.geometry("1180x840")
        self.minsize(980, 720)
        self.configure(bg=self.ui["bg"])

        self.storage = Storage()
        self.memory = ensure_memory_schema(self.storage.load_memory())

        self.log_queue: queue.Queue[str] = queue.Queue()
        self.cancel_event = threading.Event()
        self.pause_event = threading.Event()
        self.worker_thread: Optional[threading.Thread] = None
        self.mode_var = tk.StringVar(value="overlap")
        self.mode_buttons: Dict[str, PillButton] = {}
        self.toggle_chips: List[Tuple[PillButton, tk.BooleanVar]] = []
        self.mode_frames: Dict[str, tk.Frame] = {}
        self.metric_widgets: List[Tuple[tk.Frame, tk.StringVar, tk.StringVar]] = []

        self._setup_fonts()
        self._configure_styles()
        self._build_ui()
        self._load_defaults()
        self.after(150, self._drain_logs)

    def _setup_fonts(self) -> None:
        register_openai_sans_fonts()
        family = "TkDefaultFont"
        try:
            available = {name.lower(): name for name in tkfont.families()}
            family = available.get("openai sans") or available.get(".apple system ui") or available.get("helvetica neue") or family
        except Exception:
            pass
        self.font_ui = tkfont.Font(family=family, size=12)
        self.font_small = tkfont.Font(family=family, size=10)
        self.font_label = tkfont.Font(family=family, size=10, weight="bold")
        self.font_title = tkfont.Font(family=family, size=46, weight="bold")
        self.font_heading = tkfont.Font(family=family, size=10, weight="bold")
        self.font_status = tkfont.Font(family=family, size=10, weight="bold")
        self.font_caption = tkfont.Font(family=family, size=10, weight="bold")
        self.font_button = tkfont.Font(family=family, size=10, weight="bold")
        self.font_metric = tkfont.Font(family=family, size=28, weight="bold")
        self.font_logo = tkfont.Font(family=family, size=22, weight="bold")
        self.font_nav = tkfont.Font(family=family, size=11, weight="bold")

    def _configure_styles(self) -> None:
        style = ttk.Style(self)
        try:
            style.theme_use("clam")
        except Exception:
            pass
        self.option_add("*Font", self.font_ui)
        style.configure(
            "Minimal.Horizontal.TProgressbar",
            troughcolor=self.ui["progress_trough"],
            background=self.ui["progress_fill"],
            bordercolor=self.ui["progress_trough"],
            lightcolor=self.ui["progress_fill"],
            darkcolor=self.ui["progress_fill"],
            thickness=4,
        )

    def _surface(self, parent: tk.Widget, *, pad_x: int = 14, pad_y: int = 12, radius: int = 24) -> Tuple[RoundedSurface, tk.Frame]:
        shell = RoundedSurface(parent, fill=self.ui["surface"], outline=self.ui["border"], radius=radius, pad_x=pad_x, pad_y=pad_y)
        return shell, shell.content

    def _section(self, parent: tk.Widget, title: str) -> Tuple[tk.Frame, tk.Frame]:
        shell, inner = self._surface(parent)
        tk.Label(inner, text=title, font=self.font_heading, fg=self.ui["text"], bg=self.ui["surface"]).pack(anchor="w", pady=(0, 10))
        body = tk.Frame(inner, bg=self.ui["surface"])
        body.pack(fill=tk.BOTH, expand=True)
        return shell, body

    def _field_shell(self, parent: tk.Widget) -> tk.Frame:
        return tk.Frame(parent, bg=self.ui["input_bg"], highlightbackground=self.ui["input_border"], highlightthickness=1, bd=0)

    def _primary_palette(self) -> Dict[str, str]:
        return {
            "fill": self.ui["button_bg"],
            "text": self.ui["button_fg"],
            "outline": self.ui["button_bg"],
            "hover_fill": "#262626",
            "hover_text": self.ui["button_fg"],
            "hover_outline": "#262626",
        }

    def _secondary_palette(self) -> Dict[str, str]:
        return {
            "fill": self.ui["button_secondary_bg"],
            "text": self.ui["text"],
            "outline": self.ui["button_secondary_border"],
            "hover_fill": "#FFFFFF",
            "hover_text": self.ui["text"],
            "hover_outline": self.ui["button_secondary_border"],
        }

    def _chip_palette_off(self) -> Dict[str, str]:
        return {
            "fill": self.ui["chip_bg"],
            "text": self.ui["text_muted"],
            "outline": self.ui["outline_strong"],
            "hover_fill": self.ui["surface_alt"],
            "hover_text": self.ui["text"],
            "hover_outline": self.ui["text"],
        }

    def _chip_palette_on(self) -> Dict[str, str]:
        return {
            "fill": self.ui["chip_active_bg"],
            "text": self.ui["chip_active_fg"],
            "outline": self.ui["chip_active_bg"],
        }

    def _nav_palette_off(self) -> Dict[str, str]:
        return {
            "fill": self.ui["surface_section"],
            "text": self.ui["text_muted"],
            "outline": self.ui["surface_section"],
            "hover_fill": self.ui["surface_section"],
            "hover_text": self.ui["text"],
            "hover_outline": self.ui["surface_section"],
        }

    def _nav_palette_on(self) -> Dict[str, str]:
        return {
            "fill": self.ui["text"],
            "text": self.ui["surface"],
            "outline": self.ui["text"],
        }

    def _make_button(self, parent: tk.Widget, text: str, command: Callable[[], None], *, kind: str = "secondary") -> PillButton:
        palette = self._primary_palette() if kind == "primary" else self._secondary_palette()
        return PillButton(
            parent,
            text=text,
            command=command,
            font=self.font_button,
            palette_off=palette,
            height=32,
            radius=2,
            padding_x=14,
            text_offset_y=0,
            min_width=72,
        )

    def _make_toggle_chip(self, parent: tk.Widget, text: str, variable: tk.BooleanVar) -> PillButton:
        def toggle() -> None:
            variable.set(not variable.get())
            self._refresh_toggle_chips()

        btn = PillButton(
            parent,
            text=text,
            command=toggle,
            font=self.font_button,
            palette_off=self._chip_palette_off(),
            palette_on=self._chip_palette_on(),
            height=32,
            radius=2,
            padding_x=12,
            text_offset_y=0,
            min_width=68,
        )
        self.toggle_chips.append((btn, variable))
        return btn

    def _refresh_toggle_chips(self) -> None:
        for btn, variable in self.toggle_chips:
            btn.set_active(bool(variable.get()))
        self._refresh_mode_dashboard()

    def _make_mode_button(self, parent: tk.Widget, mode: str, text: str) -> PillButton:
        btn = PillButton(
            parent,
            text=text,
            command=lambda m=mode: self._select_mode(m),
            font=self.font_nav,
            palette_off=self._nav_palette_off(),
            palette_on=self._nav_palette_on(),
            height=32,
            radius=2,
            padding_x=14,
            text_offset_y=0,
            min_width=148,
            stretch=True,
            align="left",
        )
        self.mode_buttons[mode] = btn
        return btn

    def _refresh_mode_buttons(self) -> None:
        current = self.mode_var.get()
        for mode, btn in self.mode_buttons.items():
            btn.set_active(mode == current)

    def _style_text_widget(self, widget: ScrolledText) -> None:
        widget.configure(
            relief="flat",
            bd=0,
            padx=16,
            pady=16,
            wrap=tk.WORD,
            bg=self.ui["log_bg"],
            fg=self.ui["text_soft"],
            insertbackground=self.ui["text"],
            highlightbackground=self.ui["border"],
            highlightcolor=self.ui["text"],
            highlightthickness=1,
            font=self.font_ui,
            selectbackground="#EAEAE6",
        )

    def _set_status(self, text: str) -> None:
        self.after(0, lambda: self.status_var.set(text))

    def _token_label(self, parent: tk.Widget, text: str) -> tk.Label:
        return tk.Label(
            parent,
            text=text.upper(),
            font=self.font_caption,
            fg=self.ui["text_muted"],
            bg=str(parent.cget("background")),
            anchor="w",
        )

    def _flat_panel(self, parent: tk.Widget, *, bg: Optional[str] = None) -> tk.Frame:
        panel = tk.Frame(
            parent,
            bg=bg or self.ui["surface"],
            highlightbackground=self.ui["border"],
            highlightthickness=1,
            bd=0,
        )
        return panel

    def _metric_item(self, parent: tk.Widget, label: str) -> Tuple[tk.Frame, tk.StringVar, tk.StringVar]:
        value_var = tk.StringVar(value="")
        label_var = tk.StringVar(value=label)
        item = tk.Frame(parent, bg=self.ui["surface_base"], highlightbackground=self.ui["surface_base"], bd=0)
        rule = tk.Frame(item, bg=self.ui["text"], width=2, height=42)
        rule.pack(side=tk.LEFT, fill=tk.Y, padx=(0, 14))
        content = tk.Frame(item, bg=self.ui["surface_base"])
        content.pack(side=tk.LEFT)
        tk.Label(
            content,
            textvariable=label_var,
            font=self.font_caption,
            fg=self.ui["text_muted"],
            bg=self.ui["surface_base"],
            anchor="w",
        ).pack(anchor="w")
        tk.Label(
            content,
            textvariable=value_var,
            font=self.font_metric,
            fg=self.ui["text"],
            bg=self.ui["surface_base"],
            anchor="w",
        ).pack(anchor="w")
        return item, label_var, value_var

    def _labeled_entry_row(
        self,
        parent: tk.Widget,
        label: str,
        variable: tk.StringVar,
        *,
        browse_command: Optional[Callable[[], None]] = None,
        width: Optional[int] = None,
        justify: str = "left",
    ) -> tk.Entry:
        row = tk.Frame(parent, bg=str(parent.cget("background")))
        row.pack(fill=tk.X, pady=(12, 0))
        tk.Label(
            row,
            text=label,
            font=self.font_small,
            fg=self.ui["text_muted"],
            bg=str(parent.cget("background")),
            anchor="w",
        ).place(x=0, y=0)
        shell = tk.Frame(
            row,
            bg=self.ui["input_bg"],
            highlightbackground=self.ui["input_border"],
            highlightthickness=1,
            bd=0,
            height=32,
        )
        shell.pack(side=tk.LEFT, fill=tk.X, expand=True, pady=(14, 0))
        shell.pack_propagate(False)
        entry = tk.Entry(
            shell,
            textvariable=variable,
            relief="flat",
            bd=0,
            bg=self.ui["input_bg"],
            fg=self.ui["text"],
            insertbackground=self.ui["text"],
            font=self.font_ui,
            width=width,
            highlightthickness=0,
            justify=justify,
        )
        entry.pack(fill=tk.BOTH, expand=True, padx=12, pady=0)
        if browse_command:
            self._make_button(row, "Open", browse_command).pack(side=tk.LEFT, padx=(10, 0), pady=(14, 0))
        return entry

    def _build_overlap_tab(self, parent: tk.Widget) -> None:
        source_shell = self._flat_panel(parent)
        source_shell.pack(fill=tk.X)
        source_body = tk.Frame(source_shell, bg=self.ui["surface"])
        source_body.pack(fill=tk.X, padx=32, pady=32)
        self.overlap_source_dir_entry = self._labeled_entry_row(
            source_body,
            "Source",
            self.overlap_source_dir_var,
            browse_command=self.pick_overlap_source_dir,
        )

        details = tk.Frame(parent, bg=self.ui["surface_base"])
        details.pack(fill=tk.X, pady=(32, 0))
        details.columnconfigure(0, weight=1)
        details.columnconfigure(1, weight=1)

        outputs_shell = self._flat_panel(details)
        outputs_shell.grid(row=0, column=0, sticky="nsew", padx=(0, 16))
        outputs_body = tk.Frame(outputs_shell, bg=self.ui["surface"])
        outputs_body.pack(fill=tk.BOTH, expand=True, padx=32, pady=32)
        chips = tk.Frame(outputs_body, bg=self.ui["surface"])
        chips.pack(fill=tk.X)
        for index, (label, var) in enumerate([("CSV", self.ov_csv_var), ("Overlap", self.ov_overlap_var), ("Extracted Original", self.ov_eo_var)]):
            chip = self._make_toggle_chip(chips, label, var)
            chip.grid(row=index // 2, column=index % 2, sticky="w", padx=(0, 8), pady=(0, 8))

        params_shell = self._flat_panel(details)
        params_shell.grid(row=0, column=1, sticky="nsew", padx=(16, 0))
        params_body = tk.Frame(params_shell, bg=self.ui["surface"])
        params_body.pack(fill=tk.BOTH, expand=True, padx=32, pady=32)
        self.param_overlap_entry = self._labeled_entry_row(params_body, "Multiplier", self.param_overlap_multiplier_var, width=10, justify="center")
        tk.Label(params_body, textvariable=self.estimated_normal_width_var, font=self.font_ui, fg=self.ui["text_muted"], bg=self.ui["surface"]).pack(anchor="w", pady=(14, 0))

    def _build_crop_tab(self, parent: tk.Widget) -> None:
        source_shell = self._flat_panel(parent)
        source_shell.pack(fill=tk.X)
        source_body = tk.Frame(source_shell, bg=self.ui["surface"])
        source_body.pack(fill=tk.X, padx=32, pady=32)
        self.crop_source_dir_entry = self._labeled_entry_row(
            source_body,
            "Source",
            self.crop_source_dir_var,
            browse_command=self.pick_crop_source_dir,
        )

        params_shell = self._flat_panel(parent)
        params_shell.pack(fill=tk.X, pady=(32, 0))
        params_body = tk.Frame(params_shell, bg=self.ui["surface"])
        params_body.pack(fill=tk.BOTH, expand=True, padx=32, pady=32)
        self.param_crop_entry = self._labeled_entry_row(
            params_body,
            "Ratio",
            self.param_crop_ratio_var,
            width=10,
            justify="center",
        )
        tk.Label(
            params_body,
            textvariable=self.detected_crop_ratio_var,
            font=self.font_ui,
            fg=self.ui["text_muted"],
            bg=self.ui["surface"],
        ).pack(anchor="w", pady=(14, 0))

    def _build_replace_tab(self, parent: tk.Widget) -> None:
        source_shell = self._flat_panel(parent)
        source_shell.pack(fill=tk.X)
        source_body = tk.Frame(source_shell, bg=self.ui["surface"])
        source_body.pack(fill=tk.X, padx=32, pady=32)
        self.replace_cropped_dir_entry = self._labeled_entry_row(
            source_body,
            "Cropped",
            self.replace_cropped_dir_var,
            browse_command=self.pick_replace_cropped_dir,
        )
        tk.Frame(source_body, bg=self.ui["surface"], height=18).pack(fill=tk.X)
        self.replace_target_dir_entry = self._labeled_entry_row(
            source_body,
            "Replacement",
            self.replace_target_dir_var,
            browse_command=self.pick_replace_target_dir,
        )

    def _build_ui(self) -> None:
        self.csv_path_var = tk.StringVar()
        self.overlap_source_dir_var = tk.StringVar()
        self.crop_source_dir_var = tk.StringVar()
        self.replace_cropped_dir_var = tk.StringVar()
        self.replace_target_dir_var = tk.StringVar()
        self.param_overlap_multiplier_var = tk.StringVar(value="")
        self.param_crop_ratio_var = tk.StringVar(value="")
        self.estimated_normal_width_var = tk.StringVar(value="Current estimated width: -")
        self.detected_crop_ratio_var = tk.StringVar(value="Current detected ratio: -")
        self.ov_csv_var = tk.BooleanVar(value=True)
        self.ov_overlap_var = tk.BooleanVar(value=True)
        self.ov_eo_var = tk.BooleanVar(value=True)
        self.status_var = tk.StringVar(value="Ready.")

        root = tk.Frame(self, bg=self.ui["surface_base"])
        root.pack(fill=tk.BOTH, expand=True)

        sidebar = tk.Frame(root, bg=self.ui["surface_section"], width=220)
        sidebar.pack(side=tk.LEFT, fill=tk.Y)
        sidebar.pack_propagate(False)
        tk.Frame(root, bg=self.ui["border"], width=1).pack(side=tk.LEFT, fill=tk.Y)
        tk.Label(
            sidebar,
            text="MICROFICHE",
            font=self.font_logo,
            fg=self.ui["text"],
            bg=self.ui["surface_section"],
            anchor="w",
        ).pack(fill=tk.X, padx=32, pady=(32, 24))
        nav = tk.Frame(sidebar, bg=self.ui["surface_section"])
        nav.pack(fill=tk.X, padx=32)
        self._make_mode_button(nav, "overlap", "Overlap").pack(fill=tk.X)
        self._make_mode_button(nav, "crop", "Crop").pack(fill=tk.X, pady=(8, 0))
        self._make_mode_button(nav, "replace", "Replace").pack(fill=tk.X, pady=(8, 0))

        sidebar_footer = tk.Frame(sidebar, bg=self.ui["surface_section"])
        sidebar_footer.pack(side=tk.BOTTOM, fill=tk.X, padx=32, pady=32)
        tk.Frame(sidebar_footer, bg=self.ui["border"], height=1).pack(fill=tk.X, pady=(0, 14))
        tk.Label(
            sidebar_footer,
            textvariable=self.status_var,
            font=self.font_status,
            fg=self.ui["text_muted"],
            bg=self.ui["surface_section"],
            anchor="w",
            justify="left",
        ).pack(fill=tk.X)

        main = tk.Frame(root, bg=self.ui["surface_base"])
        main.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        toolbar = tk.Frame(main, bg=self.ui["surface_base"])
        toolbar.pack(fill=tk.X, padx=32, pady=(32, 0))
        tk.Frame(toolbar, bg=self.ui["surface_base"]).pack(side=tk.LEFT, fill=tk.X, expand=True)
        action_bar = tk.Frame(toolbar, bg=self.ui["surface_base"])
        action_bar.pack(side=tk.RIGHT)
        self.run_btn = self._make_button(action_bar, "Run", self.run_pipeline, kind="primary")
        self.run_btn.pack(side=tk.LEFT)
        self.pause_btn_text = tk.StringVar(value="Pause")
        self.pause_btn = self._make_button(action_bar, self.pause_btn_text.get(), self.pause_pipeline, kind="secondary")
        self.pause_btn.pack(side=tk.LEFT, padx=(8, 0))
        self.stop_btn = self._make_button(action_bar, "Stop", self.stop_pipeline, kind="secondary")
        self.stop_btn.pack(side=tk.LEFT, padx=(8, 0))

        self.mode_container = tk.Frame(main, bg=self.ui["surface_base"])
        self.mode_container.pack(fill=tk.X, padx=32, pady=(32, 0))
        self.overlap_frame = tk.Frame(self.mode_container, bg=self.ui["bg"])
        self.crop_frame = tk.Frame(self.mode_container, bg=self.ui["bg"])
        self.replace_frame = tk.Frame(self.mode_container, bg=self.ui["bg"])
        self.mode_frames = {
            "overlap": self.overlap_frame,
            "crop": self.crop_frame,
            "replace": self.replace_frame,
        }
        self._build_overlap_tab(self.overlap_frame)
        self._build_crop_tab(self.crop_frame)
        self._build_replace_tab(self.replace_frame)

        status_row = tk.Frame(main, bg=self.ui["surface_base"])
        status_row.pack(fill=tk.X, padx=32, pady=(32, 0))
        tk.Label(
            status_row,
            textvariable=self.status_var,
            font=self.font_status,
            fg=self.ui["text_muted"],
            bg=self.ui["surface_base"],
            anchor="w",
        ).pack(fill=tk.X)
        self.progress = ttk.Progressbar(main, mode="determinate", style="Minimal.Horizontal.TProgressbar")
        self.progress.pack(fill=tk.X, padx=32, pady=(8, 0))

        log_shell = self._flat_panel(main)
        log_shell.pack(fill=tk.BOTH, expand=True, padx=32, pady=(32, 32))
        log_body = tk.Frame(log_shell, bg=self.ui["surface"])
        log_body.pack(fill=tk.BOTH, expand=True, padx=0, pady=0)
        self.log_text = ScrolledText(log_body, height=18)
        self.log_text.pack(fill=tk.BOTH, expand=True, padx=32, pady=32)
        self._style_text_widget(self.log_text)

    def _load_defaults(self) -> None:
        default_dir = str(Path.cwd())
        self.overlap_source_dir_var.set(default_dir)
        self.crop_source_dir_var.set(default_dir)
        self.replace_cropped_dir_var.set(str(Path(default_dir) / "cropped"))
        self.replace_target_dir_var.set(default_dir)
        self.csv_path_var.set("")
        self.param_overlap_multiplier_var.set(f"{PY_WIDTH_OVERLAP_REL_THRESHOLD:.2f}")
        self.param_crop_ratio_var.set(f"{DEFAULT_CROP_RATIO:.3f}")
        self.estimated_normal_width_var.set("Current estimated width: -")
        self.detected_crop_ratio_var.set("Current detected ratio: -")
        for variable in [
            self.param_overlap_multiplier_var,
            self.param_crop_ratio_var,
            self.ov_csv_var,
            self.ov_overlap_var,
            self.ov_eo_var,
        ]:
            variable.trace_add("write", lambda *_args: self._refresh_mode_dashboard())
        self._apply_mode_layout()
        self._refresh_toggle_chips()

    def _parameter_override(self) -> Dict[str, float]:
        out: Dict[str, float] = {}
        try:
            overlap = float(self.param_overlap_multiplier_var.get().strip())
            if overlap > 0:
                out["overlap_multiplier"] = overlap
        except Exception:
            pass
        return out

    def _crop_ratio(self) -> float:
        value = float(self.param_crop_ratio_var.get().strip())
        if value <= 0:
            raise ValueError("Crop ratio must be greater than zero.")
        return value

    def _current_mode(self) -> str:
        return self.mode_var.get()

    def _select_mode(self, mode: str) -> None:
        self.mode_var.set(mode)
        self._apply_mode_layout()

    def _apply_mode_layout(self) -> None:
        current = self._current_mode()
        for mode, frame in self.mode_frames.items():
            if mode == current:
                if not frame.winfo_manager():
                    frame.pack(fill=tk.X, expand=False)
            else:
                if frame.winfo_manager():
                    frame.pack_forget()
        self._refresh_mode_buttons()
        self._refresh_mode_dashboard()

    def _refresh_mode_dashboard(self) -> None:
        mode = self._current_mode()
        self.title(f"{APP_NAME} · {mode.title()}")

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

    def _pick_directory_into_var(self, variable: tk.StringVar, title: str) -> None:
        d = filedialog.askdirectory(title=title)
        if d:
            variable.set(d)
            self.csv_path_var.set("")

    def pick_overlap_source_dir(self) -> None:
        self._pick_directory_into_var(self.overlap_source_dir_var, "Select Overlap Directory")

    def pick_crop_source_dir(self) -> None:
        self._pick_directory_into_var(self.crop_source_dir_var, "Select Crop Directory")

    def pick_replace_cropped_dir(self) -> None:
        self._pick_directory_into_var(self.replace_cropped_dir_var, "Select Cropped Directory")

    def pick_replace_target_dir(self) -> None:
        self._pick_directory_into_var(self.replace_target_dir_var, "Select Replacement Directory")

    def pause_pipeline(self) -> None:
        if not self.worker_thread or not self.worker_thread.is_alive():
            return
        if self.pause_event.is_set():
            self.pause_event.clear()
            self.pause_btn_text.set("Pause")
            self.pause_btn.set_text(self.pause_btn_text.get())
            self._set_status("Resumed.")
            self.log("Pipeline resumed.")
        else:
            self.pause_event.set()
            self.pause_btn_text.set("Resume")
            self.pause_btn.set_text(self.pause_btn_text.get())
            self._set_status("Paused. Waiting after the current request.")
            self.log("Pipeline paused.")

    def stop_pipeline(self) -> None:
        self.cancel_event.set()
        self.pause_event.clear()
        if hasattr(self, "pause_btn_text"):
            self.pause_btn_text.set("Pause")
            self.pause_btn.set_text(self.pause_btn_text.get())
        self._set_status("Stop requested. Finishing the current request before shutdown.")
        self.log("Stop requested.")

    def run_pipeline(self) -> None:
        if self.worker_thread and self.worker_thread.is_alive():
            messagebox.showwarning("Busy", "A pipeline is already running.")
            return

        mode = self._current_mode()
        overlap_source_dir = Path(self.overlap_source_dir_var.get().strip())
        crop_source_dir = Path(self.crop_source_dir_var.get().strip())
        replace_cropped_dir = Path(self.replace_cropped_dir_var.get().strip())
        replace_target_dir = Path(self.replace_target_dir_var.get().strip())
        recursive = True
        live_output = True
        parameter_override = self._parameter_override()

        if mode == "overlap":
            if not overlap_source_dir.exists() or not overlap_source_dir.is_dir():
                messagebox.showerror("Error", "Overlap source directory does not exist.")
                return
            batch_root = batch_root_for_path(overlap_source_dir)
            estimate_csv_path = batch_root / "estimated_widths.csv"
            problem_csv_path = batch_root / "problem_pages.csv"
            run_log_path = batch_root / f"{mode}_run_{now_file_ts()}.txt"
            self.csv_path_var.set(str(problem_csv_path))
            crop_ratio = 0.0
        elif mode == "crop":
            if not crop_source_dir.exists() or not crop_source_dir.is_dir():
                messagebox.showerror("Error", "Crop directory does not exist.")
                return
            try:
                crop_ratio = self._crop_ratio()
            except Exception as exc:
                messagebox.showerror("Error", str(exc))
                return
            cropped_dir = crop_source_dir / "cropped"
            batch_root = batch_root_for_path(crop_source_dir)
            estimate_csv_path = batch_root / "estimated_widths.csv"
            problem_csv_path = batch_root / "problem_pages.csv"
            run_log_path = cropped_dir / f"{mode}_run_{now_file_ts()}.txt"
            self.csv_path_var.set("")
        else:
            if not replace_cropped_dir.exists() or not replace_cropped_dir.is_dir():
                messagebox.showerror("Error", "Cropped directory does not exist.")
                return
            if not replace_target_dir.exists() or not replace_target_dir.is_dir():
                messagebox.showerror("Error", "Replacement directory does not exist.")
                return
            crop_ratio = 0.0
            batch_root = batch_root_for_path(replace_target_dir)
            estimate_csv_path = batch_root / "estimated_widths.csv"
            problem_csv_path = batch_root / "problem_pages.csv"
            run_log_path = replace_target_dir / f"{mode}_run_{now_file_ts()}.txt"
            self.csv_path_var.set("")

        self.cancel_event.clear()
        self.pause_event.clear()
        self.pause_btn_text.set("Pause")
        self.pause_btn.set_text(self.pause_btn_text.get())
        self.progress["value"] = 0
        self._set_status("Starting detector...")

        def progress_cb(done: int, total: int) -> None:
            def _set() -> None:
                self.progress["maximum"] = total
                self.progress["value"] = done

            self.after(0, _set)

        def worker() -> None:
            controller = PipelineController(
                cancel_event=self.cancel_event,
                pause_event=self.pause_event,
            )
            hooks = PipelineHooks(
                log=self.log,
                status=self._set_status,
                progress=progress_cb,
                overlap_estimate=lambda pdf_path, info: self.after(
                    0,
                    lambda: self.estimated_normal_width_var.set(
                        f"Current estimated width: {pdf_path.name} -> {float(info.get('baseline_body_width') or 0.0):.0f}"
                    ),
                ),
                crop_detected=lambda src_pdf, info: self.after(
                    0,
                    lambda: self.detected_crop_ratio_var.set(
                        f"Current detected ratio: {src_pdf.name} -> {float(info.get('body_ratio') or 0.0):.4f}"
                    ),
                ),
                replace_cropped_dir=lambda output_dir: self.after(
                    0,
                    lambda: self.replace_cropped_dir_var.set(str(output_dir)),
                ),
            )
            try:
                if mode == "overlap":
                    run_overlap_pipeline(
                        OverlapRunConfig(
                            source_dir=overlap_source_dir,
                            batch_root=batch_root,
                            estimate_csv_path=estimate_csv_path,
                            problem_csv_path=problem_csv_path,
                            run_log_path=run_log_path,
                            parameter_override=parameter_override,
                            recursive=recursive,
                            export_csv=self.ov_csv_var.get(),
                            export_overlap_pages=live_output and self.ov_overlap_var.get(),
                            export_extracted_original=live_output and self.ov_eo_var.get(),
                            render_dpi=220,
                        ),
                        hooks=hooks,
                        controller=controller,
                        storage=self.storage,
                        memory=self.memory,
                    )
                elif mode == "crop":
                    run_crop_pipeline(
                        CropRunConfig(
                            source_dir=crop_source_dir,
                            cropped_dir=crop_source_dir / "cropped",
                            uncropped_dir=crop_source_dir / "uncropped",
                            crop_ratio=crop_ratio,
                            run_log_path=run_log_path,
                            render_dpi=220,
                        ),
                        hooks=hooks,
                        controller=controller,
                    )
                else:
                    run_replace_pipeline(
                        ReplaceRunConfig(
                            cropped_dir=replace_cropped_dir,
                            replacement_dir=replace_target_dir,
                            run_log_path=run_log_path,
                        ),
                        hooks=hooks,
                        controller=controller,
                    )
            finally:
                self.pause_event.clear()
                self.after(0, lambda: self.pause_btn_text.set("Pause"))
                self.after(0, lambda: self.pause_btn.set_text(self.pause_btn_text.get()))
                self.after(0, lambda: self.progress.configure(value=0))

        self.worker_thread = threading.Thread(target=worker, daemon=True)
        self.worker_thread.start()

def main() -> None:
    app = App()
    app.mainloop()


if __name__ == "__main__":
    main()
