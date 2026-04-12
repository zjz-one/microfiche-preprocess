#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib.util
import json
import os
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
DelicateCropRunConfig = _APP_MODULE.DelicateCropRunConfig
JpegToPdfRunConfig = _APP_MODULE.JpegToPdfRunConfig
OverlapRunConfig = _APP_MODULE.OverlapRunConfig
PipelineController = _APP_MODULE.PipelineController
PipelineHooks = _APP_MODULE.PipelineHooks
PdfToJpegRunConfig = _APP_MODULE.PdfToJpegRunConfig
ReplaceRunConfig = _APP_MODULE.ReplaceRunConfig
resolve_delicate_output_paths = _APP_MODULE.resolve_delicate_output_paths
run_delicate_crop_pipeline = _APP_MODULE.run_delicate_crop_pipeline
resolve_manual_output_paths = _APP_MODULE.resolve_manual_output_paths
save_manual_first_page_adjustment = _APP_MODULE.save_manual_first_page_adjustment
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

    delicate_crop = subparsers.add_parser("delicate-crop")
    delicate_crop.add_argument("--file-path", action="append", required=True)
    delicate_crop.add_argument("--right-indent-pct", type=float, default=0.0)

    manual_crop = subparsers.add_parser("manual-crop")
    manual_crop.add_argument("--source-pdf", required=True)
    manual_crop.add_argument("--output-pdf")
    manual_crop.add_argument("--archive-original-to")
    manual_crop.add_argument("--rotate-degrees", type=float, default=0.0)
    manual_crop.add_argument("--trim-left", type=float, default=0.0)
    manual_crop.add_argument("--trim-top", type=float, default=0.0)
    manual_crop.add_argument("--trim-right", type=float, default=0.0)
    manual_crop.add_argument("--trim-bottom", type=float, default=0.0)

    pdf_to_jpeg = subparsers.add_parser("pdf-to-jpeg")
    pdf_to_jpeg.add_argument("--source-dir", required=True)

    jpeg_to_pdf = subparsers.add_parser("jpeg-to-pdf")
    jpeg_to_pdf.add_argument("--source-dir", required=True)

    replace = subparsers.add_parser("replace")
    replace.add_argument("--cropped-dir", required=True)
    replace.add_argument("--replacement-dir", required=True)
    replace.add_argument("--onedrive-assisted", action=argparse.BooleanOptionalAction, default=False)
    replace.add_argument("--auto-freeup", action=argparse.BooleanOptionalAction, default=True)

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


def _require_file(path: Path, label: str, writer: JsonEventWriter) -> bool:
    if path.exists() and path.is_file():
        return True
    writer.emit("error", message=f"{label} does not exist: {path}")
    return False


def _run_overlap(
    args: argparse.Namespace,
    *,
    writer: JsonEventWriter,
    hooks: PipelineHooks,
    controller: PipelineController,
    storage: Storage,
    memory,
):
    source_dir = Path(args.source_dir).expanduser()
    if not _require_directory(source_dir, "Source directory", writer):
        return None
    batch_root = batch_root_for_path(source_dir)
    return run_overlap_pipeline(
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


def _run_crop(
    args: argparse.Namespace,
    *,
    writer: JsonEventWriter,
    hooks: PipelineHooks,
    controller: PipelineController,
):
    source_dir = Path(args.source_dir).expanduser()
    if not _require_directory(source_dir, "Crop directory", writer):
        return None
    return run_crop_pipeline(
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


def _run_delicate_crop(
    args: argparse.Namespace,
    *,
    writer: JsonEventWriter,
    hooks: PipelineHooks,
    controller: PipelineController,
):
    file_paths = [Path(path).expanduser() for path in args.file_path]
    existing_paths = [path for path in file_paths if path.exists() and path.is_file()]
    if not existing_paths:
        writer.emit("error", message="No valid PDF files were provided.")
        return None
    output_paths = resolve_delicate_output_paths(existing_paths[0])
    return run_delicate_crop_pipeline(
        DelicateCropRunConfig(
            pdf_paths=existing_paths,
            right_indent_pct=float(args.right_indent_pct),
            output_root=output_paths["delicate_root"],
            run_log_path=output_paths["delicate_root"] / "delicate-crop-run-cli.txt",
        ),
        hooks=hooks,
        controller=controller,
    )


def _run_manual_crop(args: argparse.Namespace, *, writer: JsonEventWriter):
    source_pdf = Path(args.source_pdf).expanduser()
    if not _require_file(source_pdf, "Source PDF", writer):
        return None
    default_output_paths = resolve_manual_output_paths(source_pdf)
    output_pdf = Path(args.output_pdf).expanduser() if args.output_pdf else default_output_paths["mcropped_path"]
    archive_original_to = (
        Path(args.archive_original_to).expanduser() if args.archive_original_to else default_output_paths["original_save_path"]
    )
    source_for_render = source_pdf
    archived_in_advance = False
    try:
        if archive_original_to and output_pdf.resolve() == source_pdf.resolve():
            archive_original_to.parent.mkdir(parents=True, exist_ok=True)
            os.replace(str(source_pdf), str(archive_original_to))
            source_for_render = archive_original_to
            archived_in_advance = True

        info = save_manual_first_page_adjustment(
            source_for_render,
            output_pdf,
            float(args.rotate_degrees),
            float(args.trim_left),
            float(args.trim_top),
            float(args.trim_right),
            float(args.trim_bottom),
            logger=lambda message: writer.emit("log", message=message),
            render_dpi=220,
        )
    except Exception:
        if archived_in_advance and archive_original_to and archive_original_to.exists() and not source_pdf.exists():
            os.replace(str(archive_original_to), str(source_pdf))
        raise
    if archive_original_to and not archived_in_advance:
        archive_original_to.parent.mkdir(parents=True, exist_ok=True)
        os.replace(str(source_pdf), str(archive_original_to))
    return {
        "ok": True,
        "mode": "manual-crop",
        "source_pdf": str(source_pdf),
        "output_pdf": str(output_pdf),
        "archive_original_to": str(archive_original_to) if archive_original_to else "",
        **info,
    }


def _run_pdf_to_jpeg(
    args: argparse.Namespace,
    *,
    writer: JsonEventWriter,
    hooks: PipelineHooks,
    controller: PipelineController,
):
    source_dir = Path(args.source_dir).expanduser()
    if not _require_directory(source_dir, "PDF directory", writer):
        return None
    return run_pdf_to_jpeg_pipeline(
        PdfToJpegRunConfig(
            source_dir=source_dir,
            output_dir=source_dir / "JPEG",
            run_log_path=source_dir / "JPEG" / "pdf-to-jpeg-run.txt",
        ),
        hooks=hooks,
        controller=controller,
    )


def _run_jpeg_to_pdf(
    args: argparse.Namespace,
    *,
    writer: JsonEventWriter,
    hooks: PipelineHooks,
    controller: PipelineController,
):
    source_dir = Path(args.source_dir).expanduser()
    if not _require_directory(source_dir, "JPEG directory", writer):
        return None
    return run_jpeg_to_pdf_pipeline(
        JpegToPdfRunConfig(
            source_dir=source_dir,
            output_dir=source_dir / "PDF",
            run_log_path=source_dir / "PDF" / "jpeg-to-pdf-run.txt",
        ),
        hooks=hooks,
        controller=controller,
    )


def _run_replace(
    args: argparse.Namespace,
    *,
    writer: JsonEventWriter,
    hooks: PipelineHooks,
    controller: PipelineController,
):
    cropped_dir = Path(args.cropped_dir).expanduser()
    replacement_dir = Path(args.replacement_dir).expanduser()
    if not _require_directory(cropped_dir, "Cropped directory", writer):
        return None
    if not _require_directory(replacement_dir, "Replacement directory", writer):
        return None
    return run_replace_pipeline(
        ReplaceRunConfig(
            cropped_dir=cropped_dir,
            replacement_dir=replacement_dir,
            run_log_path=replacement_dir / "replace_run_cli.txt",
            onedrive_assisted=bool(args.onedrive_assisted),
            auto_freeup=bool(args.auto_freeup),
        ),
        hooks=hooks,
        controller=controller,
    )


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
        replace_step=lambda step, state, file_path, message: writer.emit(
            "replace-step",
            step=step,
            state=state,
            file_path=file_path,
            message=message,
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
    handlers = {
        "overlap": lambda: _run_overlap(
            args,
            writer=writer,
            hooks=hooks,
            controller=controller,
            storage=storage,
            memory=memory,
        ),
        "crop": lambda: _run_crop(args, writer=writer, hooks=hooks, controller=controller),
        "delicate-crop": lambda: _run_delicate_crop(args, writer=writer, hooks=hooks, controller=controller),
        "manual-crop": lambda: _run_manual_crop(args, writer=writer),
        "pdf-to-jpeg": lambda: _run_pdf_to_jpeg(args, writer=writer, hooks=hooks, controller=controller),
        "jpeg-to-pdf": lambda: _run_jpeg_to_pdf(args, writer=writer, hooks=hooks, controller=controller),
        "replace": lambda: _run_replace(args, writer=writer, hooks=hooks, controller=controller),
    }
    result = handlers[args.mode]()
    if result is None:
        return 2

    writer.emit("result", **result)
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
