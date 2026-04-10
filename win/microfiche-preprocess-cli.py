#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib.util
import json
import sys
import threading
from pathlib import Path

_APP_SCRIPT_PATH = Path(__file__).with_name("microfiche-preprocess.py")
_APP_SPEC = importlib.util.spec_from_file_location("microfiche_preprocess_app", _APP_SCRIPT_PATH)
if _APP_SPEC is None or _APP_SPEC.loader is None:
    raise RuntimeError(f"Failed to load {_APP_SCRIPT_PATH}")
_APP_MODULE = importlib.util.module_from_spec(_APP_SPEC)
sys.modules[_APP_SPEC.name] = _APP_MODULE
_APP_SPEC.loader.exec_module(_APP_MODULE)

DEFAULT_CROP_RATIO = _APP_MODULE.DEFAULT_CROP_RATIO
PY_WIDTH_OVERLAP_REL_THRESHOLD = _APP_MODULE.PY_WIDTH_OVERLAP_REL_THRESHOLD
CropRunConfig = _APP_MODULE.CropRunConfig
JpegToPdfRunConfig = _APP_MODULE.JpegToPdfRunConfig
OverlapRunConfig = _APP_MODULE.OverlapRunConfig
PipelineController = _APP_MODULE.PipelineController
PipelineHooks = _APP_MODULE.PipelineHooks
PdfToJpegRunConfig = _APP_MODULE.PdfToJpegRunConfig
ReplaceRunConfig = _APP_MODULE.ReplaceRunConfig
Storage = _APP_MODULE.Storage
batch_root_for_path = _APP_MODULE.batch_root_for_path
ensure_memory_schema = _APP_MODULE.ensure_memory_schema
run_crop_pipeline = _APP_MODULE.run_crop_pipeline
run_jpeg_to_pdf_pipeline = _APP_MODULE.run_jpeg_to_pdf_pipeline
run_overlap_pipeline = _APP_MODULE.run_overlap_pipeline
run_pdf_to_jpeg_pipeline = _APP_MODULE.run_pdf_to_jpeg_pipeline
run_replace_pipeline = _APP_MODULE.run_replace_pipeline


class JsonEventWriter:
    def __init__(self) -> None:
        self._lock = threading.Lock()

    def emit(self, event: str, **payload) -> None:
        with self._lock:
            line = json.dumps({"event": event, **payload}, ensure_ascii=False)
            sys.stdout.write(line + "\n")
            sys.stdout.flush()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="mode", required=True)

    overlap = subparsers.add_parser("overlap")
    overlap.add_argument("--source-dir", required=True)
    overlap.add_argument("--overlap-multiplier", type=float, default=PY_WIDTH_OVERLAP_REL_THRESHOLD)
    overlap.add_argument("--export-csv", action=argparse.BooleanOptionalAction, default=True)
    overlap.add_argument("--export-overlap", action=argparse.BooleanOptionalAction, default=True)
    overlap.add_argument("--export-extracted-original", action=argparse.BooleanOptionalAction, default=True)

    crop = subparsers.add_parser("crop")
    crop.add_argument("--source-dir", required=True)
    crop.add_argument("--crop-ratio", type=float, default=DEFAULT_CROP_RATIO)

    pdf_to_jpeg = subparsers.add_parser("pdf-to-jpeg")
    pdf_to_jpeg.add_argument("--source-dir", required=True)

    jpeg_to_pdf = subparsers.add_parser("jpeg-to-pdf")
    jpeg_to_pdf.add_argument("--source-dir", required=True)

    replace = subparsers.add_parser("replace")
    replace.add_argument("--cropped-dir", required=True)
    replace.add_argument("--replacement-dir", required=True)

    return parser.parse_args()


def _read_control_commands(controller: PipelineController, writer: JsonEventWriter) -> None:
    try:
        for raw in sys.stdin:
            command = raw.strip().lower()
            if not command:
                continue
            if command == "pause":
                controller.pause_event.set()
                writer.emit("control", state="paused")
            elif command == "resume":
                controller.pause_event.clear()
                writer.emit("control", state="running")
            elif command == "stop":
                controller.cancel_event.set()
                controller.pause_event.clear()
                writer.emit("control", state="stopping")
            else:
                writer.emit("control-error", message=f"Unknown control command: {command}")
    except Exception as exc:
        writer.emit("control-error", message=str(exc))


def _require_directory(path: Path, label: str, writer: JsonEventWriter) -> bool:
    if path.exists() and path.is_dir():
        return True
    writer.emit("error", message=f"{label} does not exist: {path}")
    return False


def main() -> int:
    args = _parse_args()
    writer = JsonEventWriter()
    controller = PipelineController(
        cancel_event=threading.Event(),
        pause_event=threading.Event(),
    )
    hooks = PipelineHooks(
        log=lambda message: writer.emit("log", message=message),
        status=lambda message: writer.emit("status", message=message),
        progress=lambda done, total: writer.emit("progress", done=done, total=total),
        overlap_estimate=lambda pdf_path, info: writer.emit(
            "estimate",
            file_name=pdf_path.name,
            file_path=str(pdf_path),
            baseline_body_width=float(info.get("baseline_body_width") or 0.0),
            text=f"Current estimated width: {pdf_path.name} -> {float(info.get('baseline_body_width') or 0.0):.0f}",
        ),
        replace_cropped_dir=lambda output_dir: writer.emit(
            "suggested-cropped-dir",
            path=str(output_dir),
        ),
    )

    control_thread = threading.Thread(
        target=_read_control_commands,
        args=(controller, writer),
        daemon=True,
    )
    control_thread.start()

    storage = Storage()
    memory = ensure_memory_schema(storage.load_memory())
    writer.emit("ready", mode=args.mode)

    if args.mode == "overlap":
        source_dir = Path(args.source_dir).expanduser()
        if not _require_directory(source_dir, "Source directory", writer):
            return 2
        batch_root = batch_root_for_path(source_dir)
        result = run_overlap_pipeline(
            OverlapRunConfig(
                source_dir=source_dir,
                batch_root=batch_root,
                estimate_csv_path=batch_root / "estimated_widths.csv",
                problem_csv_path=batch_root / "problem_pages.csv",
                run_log_path=batch_root / "overlap_run_cli.txt",
                parameter_override={"overlap_multiplier": float(args.overlap_multiplier)},
                export_csv=bool(args.export_csv),
                export_overlap_pages=bool(args.export_overlap),
                export_extracted_original=bool(args.export_extracted_original),
            ),
            hooks=hooks,
            controller=controller,
            storage=storage,
            memory=memory,
        )
    elif args.mode == "crop":
        source_dir = Path(args.source_dir).expanduser()
        if not _require_directory(source_dir, "Crop directory", writer):
            return 2
        result = run_crop_pipeline(
            CropRunConfig(
                source_dir=source_dir,
                cropped_dir=source_dir / "cropped",
                uncropped_dir=source_dir / "uncropped",
                crop_ratio=float(args.crop_ratio),
                run_log_path=(source_dir / "cropped" / "crop_run_cli.txt"),
            ),
            hooks=hooks,
            controller=controller,
        )
    elif args.mode == "pdf-to-jpeg":
        source_dir = Path(args.source_dir).expanduser()
        if not _require_directory(source_dir, "PDF directory", writer):
            return 2
        result = run_pdf_to_jpeg_pipeline(
            PdfToJpegRunConfig(
                source_dir=source_dir,
                output_dir=source_dir / "JPEG",
                run_log_path=source_dir / "JPEG" / "pdf-to-jpeg-run.txt",
            ),
            hooks=hooks,
            controller=controller,
        )
    elif args.mode == "jpeg-to-pdf":
        source_dir = Path(args.source_dir).expanduser()
        if not _require_directory(source_dir, "JPEG directory", writer):
            return 2
        result = run_jpeg_to_pdf_pipeline(
            JpegToPdfRunConfig(
                source_dir=source_dir,
                output_dir=source_dir / "PDF",
                run_log_path=source_dir / "PDF" / "jpeg-to-pdf-run.txt",
            ),
            hooks=hooks,
            controller=controller,
        )
    else:
        cropped_dir = Path(args.cropped_dir).expanduser()
        replacement_dir = Path(args.replacement_dir).expanduser()
        if not _require_directory(cropped_dir, "Cropped directory", writer):
            return 2
        if not _require_directory(replacement_dir, "Replacement directory", writer):
            return 2
        result = run_replace_pipeline(
            ReplaceRunConfig(
                cropped_dir=cropped_dir,
                replacement_dir=replacement_dir,
                run_log_path=replacement_dir / "replace_run_cli.txt",
            ),
            hooks=hooks,
            controller=controller,
        )

    writer.emit("result", **result)
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
