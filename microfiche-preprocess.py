#!/usr/bin/env python3
from __future__ import annotations

import csv
import ctypes
import datetime as dt
from dataclasses import dataclass
import importlib.util
import io
import json
import os
import re
import shutil
import statistics
import subprocess
import threading
import sys
import time
import traceback
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import fitz  # PyMuPDF
from PIL import Image, ImageOps

BODY_DARK_THRESHOLD = 100
BODY_COVERAGE_FRAC = 0.30
PY_WIDTH_OVERLAP_REL_THRESHOLD = 1.03
DEFAULT_CROP_RATIO = 2.242
CROPPED_FILE_PREFIX = "CR_"
CONVERT_GENERATED_DIR_NAMES = {"JPEG", "PDF"}
REPLACE_ASSIST_STEP_ORDER = [
    "hydrate",
    "verify-local",
    "replace",
    "wait-sync-idle",
    "free-up-space",
]
REPLACE_RESULT_CSV_FIELDS = [
    "cropped_file_name",
    "cropped_file_path",
    "target_pdf_path",
    "page",
    "success",
    "message",
    "onedrive_assisted",
    "hydrate",
    "verify_local",
    "replace_step",
    "wait_sync_idle",
    "free_up_space",
]
PLACEHOLDER_STATUS_FULL_PRIMARY_STREAM_AVAILABLE = 0x2
PLACEHOLDER_STATUS_CREATE_FILE_ACCESSIBLE = 0x4
PLACEHOLDER_STATUS_CLOUDFILE_PLACEHOLDER = 0x8
SYNC_STATUS_NEEDS_UPLOAD = 0x1
SYNC_STATUS_NEEDS_DOWNLOAD = 0x2
SYNC_STATUS_TRANSFERRING = 0x4
SYNC_STATUS_PAUSED = 0x8
SYNC_STATUS_HAS_ERROR = 0x10
SYNC_STATUS_FETCHING_METADATA = 0x20
SYNC_STATUS_USER_REQUESTED_REFRESH = 0x40
SYNC_STATUS_HAS_WARNING = 0x80
SYNC_STATUS_EXCLUDED = 0x100
SYNC_STATUS_INCOMPLETE = 0x200
SYNC_STATUS_PLACEHOLDER_IF_EMPTY = 0x400
SYNC_STATUS_BUSY_MASK = (
    SYNC_STATUS_NEEDS_UPLOAD
    | SYNC_STATUS_NEEDS_DOWNLOAD
    | SYNC_STATUS_TRANSFERRING
    | SYNC_STATUS_PAUSED
    | SYNC_STATUS_FETCHING_METADATA
    | SYNC_STATUS_USER_REQUESTED_REFRESH
    | SYNC_STATUS_INCOMPLETE
)
SYNC_STATUS_ERROR_MASK = SYNC_STATUS_HAS_ERROR | SYNC_STATUS_EXCLUDED
WINDOWS_FILE_ATTRIBUTE_OFFLINE = 0x00001000
WINDOWS_CF_PIN_STATE_PINNED = 1
WINDOWS_CF_PIN_STATE_UNPINNED = 2
WINDOWS_CF_SET_PIN_FLAG_NONE = 0x00000000
WINDOWS_CF_DEHYDRATE_FLAG_NONE = 0x00000000



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
    replace_cropped_dir: Optional[Callable[[Path], None]] = None
    replace_step: Optional[Callable[[str, str, str, str], None]] = None


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
class DelicateCropRunConfig:
    pdf_paths: List[Path]
    right_indent_pct: float
    run_log_path: Path
    output_root: Optional[Path] = None
    render_dpi: int = 220


@dataclass
class ReplaceRunConfig:
    cropped_dir: Path
    replacement_dir: Path
    run_log_path: Path
    result_csv_path: Optional[Path] = None
    onedrive_assisted: bool = False
    auto_freeup: bool = True


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


def _is_inside_generated_convert_dir(path: Path, root: Path) -> bool:
    try:
        rel_path = path.resolve().relative_to(root.resolve())
    except Exception:
        return False
    return bool(rel_path.parts) and rel_path.parts[0] in CONVERT_GENERATED_DIR_NAMES


def list_convert_source_pdfs(root: Path) -> List[Path]:
    return [
        pdf_path
        for pdf_path in list_pdfs(root, recursive=True)
        if not _is_inside_generated_convert_dir(pdf_path, root)
    ]


def render_page_jpeg(page: fitz.Page, dpi: int = 220, max_width: Optional[int] = 960, quality: int = 55) -> bytes:
    pix = page.get_pixmap(dpi=dpi, colorspace=fitz.csRGB)
    img = Image.frombytes("RGB", (pix.width, pix.height), pix.samples)
    if max_width and img.width > max_width:
        h = int(img.height * max_width / img.width)
        img = img.resize((max_width, h), Image.Resampling.LANCZOS)
    bio = io.BytesIO()
    img.save(bio, format="JPEG", quality=quality, optimize=True)
    return bio.getvalue()


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


def edge_strip_is_black(
    gray_img: Image.Image,
    bbox: Tuple[int, int, int, int],
    edge: str,
    dark_threshold: int = BODY_DARK_THRESHOLD,
    min_dark_frac: float = 0.85,
) -> bool:
    x0, y0, x1, y1 = bbox
    if edge == "left":
        region = (0, y0, x0, y1)
    elif edge == "right":
        region = (x1, y0, gray_img.width, y1)
    elif edge == "top":
        region = (x0, 0, x1, y0)
    elif edge == "bottom":
        region = (x0, y1, x1, gray_img.height)
    else:
        raise ValueError(f"Unsupported edge: {edge}")

    rx0, ry0, rx1, ry1 = region
    if rx1 <= rx0 or ry1 <= ry0:
        return False

    pix = gray_img.load()
    dark_count = 0
    total = 0
    for y in range(ry0, ry1):
        for x in range(rx0, rx1):
            total += 1
            if pix[x, y] <= dark_threshold:
                dark_count += 1
    if total <= 0:
        return False
    return (dark_count / total) >= min_dark_frac


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



def classify_python_page(page_cues: Dict[str, Any]) -> Dict[str, Any]:
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
                    py_result = classify_python_page(page_cues)
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
def write_replace_results_csv(rows: List[Dict[str, Any]], out_csv: Path) -> int:
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with out_csv.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=REPLACE_RESULT_CSV_FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    "cropped_file_name": row.get("cropped_file_name", ""),
                    "cropped_file_path": row.get("cropped_file_path", ""),
                    "target_pdf_path": row.get("target_pdf_path", ""),
                    "page": row.get("page", ""),
                    "success": row.get("success", ""),
                    "message": row.get("message", ""),
                    "onedrive_assisted": row.get("onedrive_assisted", ""),
                    "hydrate": row.get("hydrate", ""),
                    "verify_local": row.get("verify_local", ""),
                    "replace_step": row.get("replace_step", ""),
                    "wait_sync_idle": row.get("wait_sync_idle", ""),
                    "free_up_space": row.get("free_up_space", ""),
                }
            )
    return len(rows)


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
        "has_left_black_edge": edge_strip_is_black(gray, bbox, "left", dark_threshold=dark_threshold),
        "has_right_black_edge": edge_strip_is_black(gray, bbox, "right", dark_threshold=dark_threshold),
        "has_top_black_edge": edge_strip_is_black(gray, bbox, "top", dark_threshold=dark_threshold),
        "has_bottom_black_edge": edge_strip_is_black(gray, bbox, "bottom", dark_threshold=dark_threshold),
        "body_width": float(pdf_rect.width),
        "body_height": float(pdf_rect.height),
    }


def compute_edge_trimmed_rect(
    body_rect: fitz.Rect,
    page_rect: fitz.Rect,
    *,
    has_left_black_edge: bool,
    has_right_black_edge: bool,
    has_top_black_edge: bool,
    has_bottom_black_edge: bool,
) -> fitz.Rect:
    if body_rect.width <= 0 or body_rect.height <= 0:
        raise ValueError("Body rectangle is empty.")
    target_x0 = float(body_rect.x0) if has_left_black_edge else float(page_rect.x0)
    target_x1 = float(body_rect.x1) if has_right_black_edge else float(page_rect.x1)
    target_y0 = float(body_rect.y0) if has_top_black_edge else float(page_rect.y0)
    target_y1 = float(body_rect.y1) if has_bottom_black_edge else float(page_rect.y1)
    if target_x1 <= target_x0 or target_y1 <= target_y0:
        raise ValueError("Trimmed crop rectangle is empty.")
    return fitz.Rect(target_x0, target_y0, target_x1, target_y1)


def compute_right_indented_crop_rect(
    trimmed_rect: fitz.Rect,
    right_indent_pct: float,
) -> fitz.Rect:
    pct = float(right_indent_pct or 0.0)
    if pct < 0.0 or pct >= 100.0:
        raise ValueError("Right indent percent must be between 0 and 100.")
    inset = float(trimmed_rect.width) * pct / 100.0
    target_x1 = float(trimmed_rect.x1) - inset
    if target_x1 <= float(trimmed_rect.x0):
        raise ValueError("Right indent percent leaves no remaining page width.")
    return fitz.Rect(float(trimmed_rect.x0), float(trimmed_rect.y0), target_x1, float(trimmed_rect.y1))


def compute_left_anchored_crop_rect(
    body_rect: fitz.Rect,
    page_rect: fitz.Rect,
    crop_ratio: float,
    *,
    has_left_black_edge: bool,
    has_right_black_edge: bool,
    has_top_black_edge: bool,
    has_bottom_black_edge: bool,
) -> fitz.Rect:
    ratio = float(crop_ratio or 0.0)
    if ratio <= 0:
        raise ValueError("Crop ratio must be greater than zero.")
    trimmed_rect = compute_edge_trimmed_rect(
        body_rect,
        page_rect,
        has_left_black_edge=has_left_black_edge,
        has_right_black_edge=has_right_black_edge,
        has_top_black_edge=has_top_black_edge,
        has_bottom_black_edge=has_bottom_black_edge,
    )

    target_x0 = float(trimmed_rect.x0)
    target_y0 = float(trimmed_rect.y0)
    target_y1 = float(trimmed_rect.y1)
    target_height = float(trimmed_rect.height)
    if target_height <= 0:
        raise ValueError("Crop height is empty.")

    target_width = target_height * ratio
    target_x1 = target_x0 + target_width
    max_x1 = float(trimmed_rect.x1)
    if target_x1 > max_x1:
        raise ValueError(
            f"Crop rectangle exceeds the source page width for crop ratio {ratio:.3f}: "
            f"page_width={page_rect.width:.3f}, max_x1={max_x1:.3f}, target_x1={target_x1:.3f}"
        )
    return fitz.Rect(target_x0, target_y0, target_x1, target_y1)


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
        crop_rect = compute_left_anchored_crop_rect(
            body_info["pdf_rect"],
            page.rect,
            crop_ratio,
            has_left_black_edge=bool(body_info["has_left_black_edge"]),
            has_right_black_edge=bool(body_info["has_right_black_edge"]),
            has_top_black_edge=bool(body_info["has_top_black_edge"]),
            has_bottom_black_edge=bool(body_info["has_bottom_black_edge"]),
        )

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


def crop_pdf_with_right_indent_pct(
    src_path: Path,
    out_path: Path,
    right_indent_pct: float,
    logger,
    render_dpi: int = 220,
) -> Dict[str, Any]:
    src_path = Path(src_path)
    out_path = Path(out_path)
    temp_output = out_path.with_name(f"{out_path.stem}.delicate-crop.tmp-{now_file_ts()}{out_path.suffix}")
    try:
        doc = fitz.open(str(src_path))
    except Exception as exc:
        raise RuntimeError(f"Open failed: {exc}") from exc

    try:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        if len(doc) <= 0:
            raise ValueError("Source PDF has no pages.")
        page = doc[0]
        body_info = detect_page_body_rect(page, dpi=render_dpi)
        trimmed_rect = compute_edge_trimmed_rect(
            body_info["pdf_rect"],
            page.rect,
            has_left_black_edge=bool(body_info["has_left_black_edge"]),
            has_right_black_edge=bool(body_info["has_right_black_edge"]),
            has_top_black_edge=bool(body_info["has_top_black_edge"]),
            has_bottom_black_edge=bool(body_info["has_bottom_black_edge"]),
        )
        crop_rect = compute_right_indented_crop_rect(trimmed_rect, right_indent_pct)

        out_doc = fitz.open()
        try:
            out_page = out_doc.new_page(width=crop_rect.width, height=crop_rect.height)
            out_page.show_pdf_page(out_page.rect, doc, 0, clip=crop_rect)
            out_doc.save(str(temp_output), garbage=4, deflate=True)
        finally:
            out_doc.close()

        os.replace(str(temp_output), str(out_path))
        return {
            "body_width": round(body_info["body_width"], 3),
            "body_height": round(body_info["body_height"], 3),
            "crop_width": round(float(crop_rect.width), 3),
            "crop_height": round(float(crop_rect.height), 3),
            "right_indent_pct": round(float(right_indent_pct), 4),
        }
    except Exception:
        if temp_output.exists():
            try:
                temp_output.unlink()
            except Exception:
                logger(f"Failed to remove temporary delicate crop file: {temp_output}")
        raise
    finally:
        doc.close()


def save_manual_first_page_adjustment(
    src_path: Path,
    out_path: Path,
    rotate_degrees: float,
    trim_left_frac: float,
    trim_top_frac: float,
    trim_right_frac: float,
    trim_bottom_frac: float,
    logger,
    render_dpi: int = 220,
) -> Dict[str, Any]:
    src_path = Path(src_path)
    out_path = Path(out_path)
    rotation = float(rotate_degrees or 0.0)
    trim_left = float(trim_left_frac or 0.0)
    trim_top = float(trim_top_frac or 0.0)
    trim_right = float(trim_right_frac or 0.0)
    trim_bottom = float(trim_bottom_frac or 0.0)

    for value, name in [
        (trim_left, "left trim"),
        (trim_top, "top trim"),
        (trim_right, "right trim"),
        (trim_bottom, "bottom trim"),
    ]:
        if value < 0.0 or value >= 1.0:
            raise ValueError(f"{name.title()} must be between 0 and 1.")
    if trim_left + trim_right >= 1.0:
        raise ValueError("Left and right trims leave no remaining width.")
    if trim_top + trim_bottom >= 1.0:
        raise ValueError("Top and bottom trims leave no remaining height.")

    doc: Optional[fitz.Document] = None
    temp_path = out_path.with_name(f"{out_path.stem}.manual-crop.tmp-{now_file_ts()}{out_path.suffix}")
    try:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        doc = fitz.open(str(src_path))
        if len(doc) <= 0:
            raise ValueError("Source PDF has no pages.")
        image_bytes = render_page_jpeg(doc[0], dpi=render_dpi, max_width=100000, quality=92)
        image = Image.open(io.BytesIO(image_bytes)).convert("RGB")
        doc.close()
        doc = None

        rotated = image.rotate(
            -rotation,
            expand=True,
            resample=Image.Resampling.BICUBIC,
            fillcolor=(0, 0, 0),
        )
        x0 = int(round(rotated.width * trim_left))
        y0 = int(round(rotated.height * trim_top))
        x1 = int(round(rotated.width * (1.0 - trim_right)))
        y1 = int(round(rotated.height * (1.0 - trim_bottom)))
        if x1 <= x0 or y1 <= y0:
            raise ValueError("Manual crop produced an empty page.")

        cropped = rotated.crop((x0, y0, x1, y1))
        image_buffer = io.BytesIO()
        cropped.save(image_buffer, format="JPEG", quality=90, optimize=True)

        out_doc = fitz.open()
        try:
            pdf_width = cropped.width * 72.0 / render_dpi
            pdf_height = cropped.height * 72.0 / render_dpi
            out_page = out_doc.new_page(width=pdf_width, height=pdf_height)
            out_page.insert_image(out_page.rect, stream=image_buffer.getvalue())
            out_doc.save(str(temp_path), garbage=4, deflate=True)
        finally:
            out_doc.close()

        os.replace(str(temp_path), str(out_path))
        return {
            "rotation": round(rotation, 3),
            "trim_left_pct": round(trim_left * 100.0, 3),
            "trim_top_pct": round(trim_top * 100.0, 3),
            "trim_right_pct": round(trim_right * 100.0, 3),
            "trim_bottom_pct": round(trim_bottom * 100.0, 3),
            "crop_width": cropped.width,
            "crop_height": cropped.height,
        }
    except Exception:
        if temp_path.exists():
            try:
                temp_path.unlink()
            except Exception:
                logger(f"Failed to remove temporary manual crop file: {temp_path}")
        raise
    finally:
        if doc is not None:
            doc.close()


def build_cropped_output_name(src_name: str) -> str:
    base = str(src_name or "").strip()
    if base.startswith(CROPPED_FILE_PREFIX):
        return base
    return f"{CROPPED_FILE_PREFIX}{base}"


def resolve_cropped_workspace_dir(src_pdf: Path) -> Path:
    src_pdf = Path(src_pdf).expanduser().resolve()
    for parent in [src_pdf.parent, *src_pdf.parents]:
        if parent.name.lower() == "cropped":
            return parent
    return src_pdf.parent / "cropped"


def resolve_delicate_output_paths(src_pdf: Path) -> Dict[str, Path]:
    src_pdf = Path(src_pdf).expanduser().resolve()
    cropped_dir = resolve_cropped_workspace_dir(src_pdf)
    delicate_root = cropped_dir / "d-cropped"
    original_dir = delicate_root / "original"
    delicate_dir = delicate_root / "d-cropped"
    return {
        "cropped_dir": cropped_dir,
        "delicate_root": delicate_root,
        "original_dir": original_dir,
        "delicate_dir": delicate_dir,
        "delicate_path": delicate_dir / src_pdf.name,
        "original_path": original_dir / src_pdf.name,
    }


def resolve_manual_output_paths(src_pdf: Path) -> Dict[str, Path]:
    src_pdf = Path(src_pdf).expanduser().resolve()
    cropped_dir = resolve_cropped_workspace_dir(src_pdf)
    manual_root = cropped_dir / "m-cropped"
    mcropped_dir = manual_root / "m-cropped"
    original_save_dir = manual_root / "original"
    return {
        "cropped_dir": cropped_dir,
        "manual_root": manual_root,
        "mcropped_dir": mcropped_dir,
        "original_save_dir": original_save_dir,
        "mcropped_path": mcropped_dir / src_pdf.name,
        "original_save_path": original_save_dir / src_pdf.name,
    }


def emit_replace_step(
    hooks: PipelineHooks,
    step: str,
    state: str,
    file_path: Path,
    message: str,
    logger: Callable[[str], None],
) -> None:
    if state != "pending" or message:
        logger(f"[{step}] {state}: {file_path} {message}".rstrip())
    if hooks.replace_step is not None:
        hooks.replace_step(step, state, str(file_path), message)


def detect_windows_powershell() -> str:
    if os.name != "nt":
        raise RuntimeError("OneDrive assisted replace is only available on Windows.")
    for candidate in ("powershell.exe", "pwsh.exe"):
        if shutil.which(candidate):
            return candidate
    raise RuntimeError("Windows PowerShell was not found.")


def run_windows_shell_property_query(path: Path, property_name: str) -> Optional[int]:
    powershell = detect_windows_powershell()
    script = (
        "$ErrorActionPreference = 'Stop';"
        "$path = $args[0];"
        "$propertyName = $args[1];"
        "$shell = New-Object -ComObject Shell.Application;"
        "$folderPath = Split-Path -LiteralPath $path -Parent;"
        "$fileName = Split-Path -LiteralPath $path -Leaf;"
        "$folder = $shell.Namespace($folderPath);"
        "if ($null -eq $folder) { exit 3 };"
        "$item = $folder.ParseName($fileName);"
        "if ($null -eq $item) { exit 4 };"
        "$value = $item.ExtendedProperty($propertyName);"
        "if ($null -eq $value) { Write-Output '' } else { Write-Output $value.ToString() }"
    )
    creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
    completed = subprocess.run(
        [powershell, "-NoProfile", "-Command", script, str(path), property_name],
        capture_output=True,
        text=True,
        timeout=20,
        creationflags=creationflags,
    )
    if completed.returncode not in (0, 3, 4):
        raise RuntimeError(
            f"Shell property query failed for {path.name} ({property_name}): "
            f"{completed.stderr.strip() or completed.stdout.strip() or completed.returncode}"
        )
    raw_value = (completed.stdout or "").strip()
    if not raw_value:
        return None
    try:
        return int(raw_value)
    except Exception as exc:
        raise RuntimeError(
            f"Shell property {property_name} returned a non-integer value for {path.name}: {raw_value}"
        ) from exc


def get_windows_placeholder_status(path: Path) -> Optional[int]:
    return run_windows_shell_property_query(path, "System.FilePlaceholderStatus")


def get_windows_sync_transfer_status(path: Path) -> Optional[int]:
    return run_windows_shell_property_query(path, "System.SyncTransferStatus")


def is_cloud_managed_path(path: Path) -> bool:
    placeholder_status = get_windows_placeholder_status(path)
    if placeholder_status is not None and (placeholder_status & PLACEHOLDER_STATUS_CLOUDFILE_PLACEHOLDER):
        return True
    return get_windows_sync_transfer_status(path) is not None


def open_windows_file_handle(path: Path) -> ctypes.c_void_p:
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    create_file = kernel32.CreateFileW
    create_file.argtypes = [
        ctypes.c_wchar_p,
        ctypes.c_uint32,
        ctypes.c_uint32,
        ctypes.c_void_p,
        ctypes.c_uint32,
        ctypes.c_uint32,
        ctypes.c_void_p,
    ]
    create_file.restype = ctypes.c_void_p
    handle = create_file(
        str(path),
        0x80000000 | 0x40000000,
        0x00000001 | 0x00000002 | 0x00000004,
        None,
        3,
        0,
        None,
    )
    invalid_handle = ctypes.c_void_p(-1).value
    if handle == invalid_handle:
        raise ctypes.WinError(ctypes.get_last_error())
    return ctypes.c_void_p(handle)


def close_windows_handle(handle: ctypes.c_void_p) -> None:
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    close_handle = kernel32.CloseHandle
    close_handle.argtypes = [ctypes.c_void_p]
    close_handle.restype = ctypes.c_int
    if not close_handle(handle):
        raise ctypes.WinError(ctypes.get_last_error())


def windows_hresult_check(result: int, action: str, path: Path) -> None:
    if result == 0:
        return
    raise OSError(f"{action} failed for {path}: HRESULT 0x{result & 0xFFFFFFFF:08X}")


def windows_hydrate_placeholder(path: Path) -> None:
    handle = open_windows_file_handle(path)
    try:
        cldapi = ctypes.WinDLL("CldApi", use_last_error=True)
        hydrate = cldapi.CfHydratePlaceholder
        hydrate.argtypes = [
            ctypes.c_void_p,
            ctypes.c_longlong,
            ctypes.c_longlong,
            ctypes.c_uint32,
            ctypes.c_void_p,
        ]
        hydrate.restype = ctypes.c_long
        result = hydrate(handle, 0, -1, 0, None)
        windows_hresult_check(int(result), "Hydrate placeholder", path)
    finally:
        close_windows_handle(handle)


def windows_set_pin_state(path: Path, pin_state: int) -> None:
    handle = open_windows_file_handle(path)
    try:
        cldapi = ctypes.WinDLL("CldApi", use_last_error=True)
        set_pin = cldapi.CfSetPinState
        set_pin.argtypes = [
            ctypes.c_void_p,
            ctypes.c_uint32,
            ctypes.c_uint32,
            ctypes.c_void_p,
        ]
        set_pin.restype = ctypes.c_long
        result = set_pin(handle, pin_state, WINDOWS_CF_SET_PIN_FLAG_NONE, None)
        windows_hresult_check(int(result), "Set pin state", path)
    finally:
        close_windows_handle(handle)


def windows_dehydrate_placeholder(path: Path) -> None:
    handle = open_windows_file_handle(path)
    try:
        cldapi = ctypes.WinDLL("CldApi", use_last_error=True)
        dehydrate = cldapi.CfDehydratePlaceholder
        dehydrate.argtypes = [
            ctypes.c_void_p,
            ctypes.c_longlong,
            ctypes.c_longlong,
            ctypes.c_uint32,
            ctypes.c_void_p,
        ]
        dehydrate.restype = ctypes.c_long
        result = dehydrate(handle, 0, -1, WINDOWS_CF_DEHYDRATE_FLAG_NONE, None)
        windows_hresult_check(int(result), "Dehydrate placeholder", path)
    finally:
        close_windows_handle(handle)


def verify_windows_file_is_local(path: Path) -> None:
    placeholder_status = get_windows_placeholder_status(path)
    if placeholder_status is not None and (placeholder_status & PLACEHOLDER_STATUS_CLOUDFILE_PLACEHOLDER):
        if not (placeholder_status & PLACEHOLDER_STATUS_FULL_PRIMARY_STREAM_AVAILABLE):
            raise RuntimeError(f"{path.name} is not fully hydrated locally.")
        if not (placeholder_status & PLACEHOLDER_STATUS_CREATE_FILE_ACCESSIBLE):
            raise RuntimeError(f"{path.name} is not yet file-accessible.")
    with fitz.open(str(path)) as doc:
        if len(doc) <= 0:
            raise RuntimeError(f"{path.name} has no pages after local verification.")


def wait_for_windows_sync_idle(path: Path, *, timeout_seconds: float = 180.0) -> Optional[int]:
    t0 = time.perf_counter()
    idle_samples = 0
    while time.perf_counter() - t0 <= timeout_seconds:
        sync_status = get_windows_sync_transfer_status(path)
        if sync_status is None:
            time.sleep(1.0)
            continue
        if sync_status & SYNC_STATUS_ERROR_MASK:
            raise RuntimeError(f"{path.name} sync status reported error: {sync_status}")
        if sync_status & SYNC_STATUS_BUSY_MASK:
            idle_samples = 0
            time.sleep(1.0)
            continue
        idle_samples += 1
        if idle_samples >= 2:
            return sync_status
        time.sleep(1.0)
    raise TimeoutError(f"Timed out waiting for OneDrive sync idle: {path}")


def wait_for_windows_free_up_space(path: Path, *, timeout_seconds: float = 120.0) -> None:
    t0 = time.perf_counter()
    while time.perf_counter() - t0 <= timeout_seconds:
        placeholder_status = get_windows_placeholder_status(path)
        if placeholder_status is not None:
            if not (placeholder_status & PLACEHOLDER_STATUS_FULL_PRIMARY_STREAM_AVAILABLE):
                return
        attrs = os.stat(path).st_file_attributes if hasattr(os.stat(path), "st_file_attributes") else 0
        if attrs & WINDOWS_FILE_ATTRIBUTE_OFFLINE:
            return
        time.sleep(1.0)
    raise TimeoutError(f"Timed out waiting for free-up-space completion: {path}")


def build_replace_step_status_row(step_states: Dict[str, str]) -> Dict[str, str]:
    return {
        "hydrate": step_states.get("hydrate", ""),
        "verify_local": step_states.get("verify-local", ""),
        "replace_step": step_states.get("replace", ""),
        "wait_sync_idle": step_states.get("wait-sync-idle", ""),
        "free_up_space": step_states.get("free-up-space", ""),
    }


def run_onedrive_assisted_replace(
    target_pdf: Path,
    replacement_page_path: Path,
    page_no: int,
    hooks: PipelineHooks,
    logger: Callable[[str], None],
    *,
    auto_freeup: bool,
) -> Dict[str, str]:
    if os.name != "nt":
        raise RuntimeError("OneDrive assisted replace is only supported on Windows.")

    step_states = {step: "pending" for step in REPLACE_ASSIST_STEP_ORDER}

    def set_step(step: str, state: str, message: str) -> None:
        step_states[step] = state
        emit_replace_step(hooks, step, state, target_pdf, message, logger)

    for step in REPLACE_ASSIST_STEP_ORDER:
        set_step(step, "pending", "")

    cloud_managed = is_cloud_managed_path(target_pdf)

    if cloud_managed:
        set_step("hydrate", "active", "starting")
        placeholder_status = get_windows_placeholder_status(target_pdf)
        if placeholder_status is not None and (placeholder_status & PLACEHOLDER_STATUS_FULL_PRIMARY_STREAM_AVAILABLE):
            set_step("hydrate", "done", "already local")
        else:
            windows_hydrate_placeholder(target_pdf)
            set_step("hydrate", "done", "hydrated")
    else:
        set_step("hydrate", "skipped", "not cloud managed")

    set_step("verify-local", "active", "starting")
    verify_windows_file_is_local(target_pdf)
    set_step("verify-local", "done", "verified")

    set_step("replace", "active", "starting")
    replace_pdf_page_with_single_page(
        target_pdf,
        replacement_page_path,
        page_no=page_no,
        logger=logger,
    )
    set_step("replace", "done", "replaced")

    if cloud_managed:
        set_step("wait-sync-idle", "active", "starting")
        sync_status = wait_for_windows_sync_idle(target_pdf)
        set_step("wait-sync-idle", "done", f"status={sync_status or 0}")
    else:
        set_step("wait-sync-idle", "skipped", "not cloud managed")

    if cloud_managed and auto_freeup:
        set_step("free-up-space", "active", "starting")
        windows_set_pin_state(target_pdf, WINDOWS_CF_PIN_STATE_UNPINNED)
        windows_dehydrate_placeholder(target_pdf)
        wait_for_windows_free_up_space(target_pdf)
        set_step("free-up-space", "done", "freed")
    elif cloud_managed:
        set_step("free-up-space", "skipped", "disabled")
    else:
        set_step("free-up-space", "skipped", "not cloud managed")

    return step_states


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


def export_overlap_pages(records: List[Dict[str, Any]], logger) -> int:
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
        src_pdfs = list_convert_source_pdfs(config.source_dir)
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
                if hooks.replace_cropped_dir:
                    hooks.replace_cropped_dir(config.cropped_dir)
                runlog(
                    f"Cropped {src_pdf.name} -> {out_pdf} "
                    f"(crop_ratio={info['crop_ratio']})"
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


def run_delicate_crop_pipeline(
    config: DelicateCropRunConfig,
    hooks: PipelineHooks,
    controller: PipelineController,
) -> Dict[str, Any]:
    t0 = time.perf_counter()
    run_log_fh, runlog = _open_run_log(config.run_log_path, hooks.log)
    try:
        runlog("Detector started.")
        pdf_paths: List[Path] = []
        seen: set[str] = set()
        for raw_path in config.pdf_paths:
            path = Path(raw_path).expanduser().resolve()
            if not path.is_file() or path.suffix.lower() != ".pdf":
                continue
            key = str(path)
            if key in seen:
                continue
            seen.add(key)
            pdf_paths.append(path)

        if not pdf_paths:
            hooks.status("No delicate crop PDFs found.")
            runlog("No PDF files provided for delicate crop.")
            return {"ok": True, "mode": "delicate-crop", "status": "empty"}

        hooks.status(f"Delicate cropping {len(pdf_paths)} PDFs...")
        runlog("Mode: delicate-crop")
        runlog(f"File count: {len(pdf_paths)}")
        runlog(f"Right indent percent: {config.right_indent_pct}")
        runlog(f"Run log: {config.run_log_path}")
        hooks.progress(0, max(len(pdf_paths), 1))
        updated_count = 0
        error_count = 0
        updated_paths: List[str] = []
        failed_paths: List[str] = []

        for index, src_pdf in enumerate(pdf_paths, start=1):
            controller.wait_if_paused()
            if controller.cancel_event.is_set():
                runlog("Delicate crop stopped before completion.")
                break

            hooks.status(f"Delicate cropping {src_pdf.name}")
            try:
                output_paths = resolve_delicate_output_paths(src_pdf)
                source_for_render = src_pdf
                output_pdf = output_paths["delicate_path"]
                archive_original_to = output_paths["original_path"]
                archived_in_advance = False
                try:
                    if output_pdf.resolve() == src_pdf.resolve():
                        archive_original_to.parent.mkdir(parents=True, exist_ok=True)
                        os.replace(str(src_pdf), str(archive_original_to))
                        source_for_render = archive_original_to
                        archived_in_advance = True

                    info = crop_pdf_with_right_indent_pct(
                        source_for_render,
                        out_path=output_pdf,
                        right_indent_pct=config.right_indent_pct,
                        logger=runlog,
                        render_dpi=config.render_dpi,
                    )
                except Exception:
                    if archived_in_advance and archive_original_to.exists() and not src_pdf.exists():
                        os.replace(str(archive_original_to), str(src_pdf))
                    raise
                if not archived_in_advance:
                    archive_original_to.parent.mkdir(parents=True, exist_ok=True)
                    os.replace(str(src_pdf), str(archive_original_to))
                updated_count += 1
                updated_paths.append(str(src_pdf))
                runlog(
                    f"Delicate cropped {src_pdf.name} -> {output_pdf} "
                    f"(right_indent_pct={info['right_indent_pct']})"
                )
            except Exception as exc:
                error_count += 1
                failed_paths.append(str(src_pdf))
                runlog(f"Delicate crop failed {src_pdf.name}: {exc}")
            hooks.progress(index, max(len(pdf_paths), 1))

        elapsed = time.perf_counter() - t0
        total_count = len(pdf_paths)
        uncropped_count = max(total_count - updated_count, 0)
        runlog(f"Updated files: {updated_count}")
        runlog(f"Uncropped files: {uncropped_count}")
        runlog(f"Errors: {error_count}")
        runlog(f"Elapsed seconds: {elapsed:.2f}")
        if controller.cancel_event.is_set():
            hooks.status(f"Delicate crop stopped. updated={updated_count}, errors={error_count}")
            cancelled = True
        else:
            hooks.status(f"Delicate crop finished. updated={updated_count}, errors={error_count}")
            runlog("Pipeline finished.")
            cancelled = False
        return {
            "ok": True,
            "mode": "delicate-crop",
            "cancelled": cancelled,
            "total_count": total_count,
            "updated_count": updated_count,
            "uncropped_count": uncropped_count,
            "error_count": error_count,
            "updated_paths": updated_paths,
            "failed_paths": failed_paths,
            "elapsed_seconds": round(elapsed, 2),
            "run_log_path": str(config.run_log_path),
        }
    except Exception:
        hooks.status("Pipeline crashed. Check the execution log.")
        runlog("Pipeline crashed:\n" + traceback.format_exc())
        return {
            "ok": False,
            "mode": "delicate-crop",
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
        runlog(f"OneDrive assisted replace: {config.onedrive_assisted}")
        runlog(f"Auto free-up-space: {config.auto_freeup}")
        if config.onedrive_assisted and os.name != "nt":
            raise RuntimeError("OneDrive assisted replace is only supported on Windows.")
        runlog(f"Run log: {config.run_log_path}")
        result_csv_path = config.result_csv_path or (config.replacement_dir / "replace-results.csv")
        runlog(f"Result CSV: {result_csv_path}")
        hooks.progress(0, max(len(src_pdfs), 1))
        replaced_count = 0
        error_count = 0
        result_rows: List[Dict[str, Any]] = []

        for index, cropped_pdf in enumerate(src_pdfs, start=1):
            controller.wait_if_paused()
            if controller.cancel_event.is_set():
                runlog("Replace stopped before completion.")
                break

            hooks.status(f"Replacing {cropped_pdf.name}")
            step_states = {step: "" for step in REPLACE_ASSIST_STEP_ORDER}
            target_pdf: Optional[Path] = None
            try:
                meta = parse_tagged_source_pdf_path(cropped_pdf)
                target_pdf = find_replacement_target(cropped_pdf, config.replacement_dir)
                if config.onedrive_assisted:
                    step_states = run_onedrive_assisted_replace(
                        target_pdf,
                        cropped_pdf,
                        page_no=int(meta["page"]),
                        hooks=hooks,
                        logger=runlog,
                        auto_freeup=config.auto_freeup,
                    )
                else:
                    replace_pdf_page_with_single_page(
                        target_pdf,
                        cropped_pdf,
                        page_no=int(meta["page"]),
                        logger=runlog,
                    )
                    step_states["replace"] = "done"
                replaced_count += 1
                result_rows.append(
                    {
                        "cropped_file_name": cropped_pdf.name,
                        "cropped_file_path": str(cropped_pdf),
                        "target_pdf_path": str(target_pdf),
                        "page": int(meta["page"]),
                        "success": "true",
                        "message": "replaced",
                        "onedrive_assisted": str(config.onedrive_assisted).lower(),
                        **build_replace_step_status_row(step_states),
                    }
                )
                runlog(
                    f"Replaced {target_pdf} page {meta['page']} "
                    f"from {cropped_pdf.name}"
                )
            except Exception as exc:
                error_count += 1
                target_for_step = target_pdf or cropped_pdf
                if config.onedrive_assisted:
                    failing_step = next((step for step in REPLACE_ASSIST_STEP_ORDER if step_states.get(step) == "active"), None)
                    if failing_step:
                        step_states[failing_step] = "failed"
                        emit_replace_step(hooks, failing_step, "failed", target_for_step, str(exc), runlog)
                result_rows.append(
                    {
                        "cropped_file_name": cropped_pdf.name,
                        "cropped_file_path": str(cropped_pdf),
                        "target_pdf_path": str(target_pdf) if target_pdf else "",
                        "page": "",
                        "success": "false",
                        "message": str(exc),
                        "onedrive_assisted": str(config.onedrive_assisted).lower(),
                        **build_replace_step_status_row(step_states),
                    }
                )
                runlog(f"Replace failed {cropped_pdf.name}: {exc}")
            hooks.progress(index, max(len(src_pdfs), 1))

        elapsed = time.perf_counter() - t0
        write_replace_results_csv(result_rows, result_csv_path)
        runlog(f"Replaced pages: {replaced_count}")
        runlog(f"Replace errors: {error_count}")
        runlog(f"Replace result rows: {len(result_rows)}")
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
            "result_csv_path": str(result_csv_path),
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
        src_pdfs = list_convert_source_pdfs(config.source_dir)
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
            rel_parent = src_pdf.parent.relative_to(config.source_dir)
            out_path = config.output_dir / rel_parent / f"{src_pdf.stem}.jpg"
            try:
                doc = fitz.open(str(src_pdf))
            except Exception as exc:
                error_count += 1
                runlog(f"Open failed {src_pdf.name}: {exc}")
                hooks.progress(index, max(len(src_pdfs), 1))
                continue

            try:
                out_path.parent.mkdir(parents=True, exist_ok=True)
                page = doc[0]
                image_bytes = render_page_jpeg(
                    page,
                    dpi=config.render_dpi,
                    max_width=None,
                    quality=config.quality,
                )
                temp_path = out_path.with_name(f"{out_path.stem}.tmp-{now_file_ts()}{out_path.suffix}")
                try:
                    temp_path.write_bytes(image_bytes)
                    os.replace(str(temp_path), str(out_path))
                except Exception:
                    if temp_path.exists():
                        try:
                            temp_path.unlink()
                        except Exception:
                            runlog(f"Failed to remove temporary JPEG file: {temp_path}")
                    raise
                created_count += 1
                if len(doc) > 1:
                    runlog(f"Converted {src_pdf.name} -> {out_path.name} using page 1 of {len(doc)}")
                else:
                    runlog(f"Converted {src_pdf.name} -> {out_path.name}")
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
        src_images = list_jpegs(config.source_dir, recursive=True)
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
            rel_parent = src_image.parent.relative_to(config.source_dir)
            out_path = config.output_dir / rel_parent / f"{src_image.stem}.pdf"
            temp_path = out_path.with_name(f"{out_path.stem}.tmp-{now_file_ts()}{out_path.suffix}")
            try:
                out_path.parent.mkdir(parents=True, exist_ok=True)
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
def main() -> int:
    cli_path = Path(__file__).with_name("microfiche-preprocess-cli.py")
    spec = importlib.util.spec_from_file_location("microfiche_preprocess_cli_main", cli_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Failed to load {cli_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return int(module.main())


if __name__ == "__main__":
    raise SystemExit(main())
