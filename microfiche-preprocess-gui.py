#!/usr/bin/env python3
from __future__ import annotations

import importlib.util
import io
import json
import sys
from pathlib import Path
from typing import Any

from PIL import Image
from PySide6.QtCore import QPointF, QProcess, QRectF, Qt, Signal
from PySide6.QtGui import QColor, QImage, QPainter, QPainterPath, QPen, QTransform
from PySide6.QtWidgets import (
    QApplication,
    QCheckBox,
    QFileDialog,
    QFrame,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QListWidget,
    QListWidgetItem,
    QMainWindow,
    QMessageBox,
    QPlainTextEdit,
    QProgressBar,
    QPushButton,
    QRadioButton,
    QSizePolicy,
    QTabWidget,
    QVBoxLayout,
    QWidget,
)

APP_NAME = "microfiche-preprocess"
DEFAULT_CROP_RATIO = "2.242"
DEFAULT_OVERLAP_MULTIPLIER = "1.03"
DEFAULT_DELICATE_RIGHT_INDENT_PCT = "0"
FIELD_INPUT_WIDTH = 96
CLI_MODES = {
    "overlap",
    "crop",
    "delicate-crop",
    "manual-crop",
    "pdf-to-jpeg",
    "jpeg-to-pdf",
    "replace",
}
TAB_MODE_BY_LABEL = {
    "OVERLAP": "overlap",
    "CROP": "crop",
    "DELICATE CROP": "delicate-crop",
    "PLAYBOARD": "playboard",
    "CONVERT": "convert",
    "REPLACE": "replace",
}
OUTPUT_GROUP_BY_OPERATION = {
    "overlap": "overlap",
    "crop": "crop",
    "delicate-crop": "delicate",
    "manual-crop": "delicate",
    "pdf-to-jpeg": "convert",
    "jpeg-to-pdf": "convert",
    "replace": "replace",
}
REPLACE_STEP_LABELS = {
    "hydrate": "HYDRATE",
    "verify-local": "VERIFY LOCAL",
    "replace": "REPLACE",
    "wait-sync-idle": "WAIT SYNC IDLE",
    "free-up-space": "FREE-UP-SPACE",
}

_BACKEND_MODULE: Any | None = None
_PLAYBOARD_MODULE: Any | None = None


def resolve_script_dir() -> Path:
    if getattr(sys, "frozen", False):
        return Path(getattr(sys, "_MEIPASS", Path(sys.executable).resolve().parent))
    return Path(__file__).resolve().parent


def resolve_cli_path() -> Path:
    return resolve_script_dir() / "microfiche-preprocess-cli.py"


def resolve_backend_path() -> Path:
    return resolve_script_dir() / "microfiche-preprocess.py"


def resolve_playboard_path() -> Path:
    return resolve_script_dir() / "pdf-playboard-gui.py"


def resolve_python_path() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable)
    project_root = Path(__file__).resolve().parent
    venv_python = project_root / ".venv" / "bin" / "python"
    if venv_python.exists():
        return venv_python
    return Path(sys.executable)


def _load_module_from_path(module_name: str, path: Path) -> Any:
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Failed to load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def load_backend_module() -> Any:
    global _BACKEND_MODULE
    if _BACKEND_MODULE is not None:
        return _BACKEND_MODULE
    backend_path = resolve_backend_path()
    module = _load_module_from_path("microfiche_preprocess_backend", backend_path)
    _BACKEND_MODULE = module
    return module


def load_playboard_module() -> Any:
    global _PLAYBOARD_MODULE
    if _PLAYBOARD_MODULE is not None:
        return _PLAYBOARD_MODULE
    playboard_path = resolve_playboard_path()
    module = _load_module_from_path("pdf_playboard_gui", playboard_path)
    _PLAYBOARD_MODULE = module
    return module


def run_cli_main() -> int:
    cli_path = resolve_cli_path()
    module = _load_module_from_path("microfiche_preprocess_cli", cli_path)
    return int(module.main())


def parse_float(text: str, label: str, *, minimum: float | None = None, maximum: float | None = None) -> float:
    try:
        value = float((text or "").strip())
    except Exception as exc:
        raise ValueError(f"{label} must be a number.") from exc
    if minimum is not None and value < minimum:
        raise ValueError(f"{label} must be at least {minimum}.")
    if maximum is not None and value > maximum:
        raise ValueError(f"{label} must be at most {maximum}.")
    return value


def extract_pdf_paths(mime_data) -> list[str]:
    if not mime_data.hasUrls():
        return []
    out: list[str] = []
    for url in mime_data.urls():
        if not url.isLocalFile():
            continue
        path = Path(url.toLocalFile()).expanduser()
        if path.suffix.lower() != ".pdf":
            continue
        out.append(str(path))
    return out


def load_pdf_preview_bundle(pdf_path: str | Path, *, dpi: int = 110, max_width: int = 900) -> dict[str, Any]:
    backend = load_backend_module()
    pdf_path = Path(pdf_path).expanduser().resolve()
    doc = backend.fitz.open(str(pdf_path))
    try:
        if len(doc) <= 0:
            raise ValueError("Source PDF has no pages.")
        page = doc[0]
        image_bytes = backend.render_page_jpeg(page, dpi=dpi, max_width=max_width, quality=72)
    finally:
        doc.close()

    qimage = QImage.fromData(image_bytes, "JPEG")
    if qimage.isNull():
        raise RuntimeError(f"Failed to render preview for {pdf_path.name}")

    gray = Image.open(io.BytesIO(image_bytes)).convert("L")
    bbox = backend.compute_page_body_bbox(
        gray,
        dark_threshold=backend.BODY_DARK_THRESHOLD,
        coverage_frac=backend.BODY_COVERAGE_FRAC,
    )
    if bbox:
        has_left = backend.edge_strip_is_black(gray, bbox, "left", dark_threshold=backend.BODY_DARK_THRESHOLD)
        has_right = backend.edge_strip_is_black(gray, bbox, "right", dark_threshold=backend.BODY_DARK_THRESHOLD)
        has_top = backend.edge_strip_is_black(gray, bbox, "top", dark_threshold=backend.BODY_DARK_THRESHOLD)
        has_bottom = backend.edge_strip_is_black(gray, bbox, "bottom", dark_threshold=backend.BODY_DARK_THRESHOLD)
        base_x0 = float(bbox[0]) if has_left else 0.0
        base_y0 = float(bbox[1]) if has_top else 0.0
        base_x1 = float(bbox[2]) if has_right else float(qimage.width())
        base_y1 = float(bbox[3]) if has_bottom else float(qimage.height())
        trimmed_rect = QRectF(base_x0, base_y0, max(1.0, base_x1 - base_x0), max(1.0, base_y1 - base_y0))
    else:
        trimmed_rect = QRectF(0.0, 0.0, float(qimage.width()), float(qimage.height()))

    return {
        "pdf_path": pdf_path,
        "image": qimage,
        "trimmed_rect": trimmed_rect,
        "mtime_ns": pdf_path.stat().st_mtime_ns,
    }


def compute_right_indented_image_rect(trimmed_rect: QRectF, right_indent_pct: float) -> QRectF:
    pct = float(right_indent_pct)
    if pct < 0.0 or pct >= 100.0:
        raise ValueError("Right indent percent must be between 0 and 100.")
    inset = trimmed_rect.width() * pct / 100.0
    target_width = trimmed_rect.width() - inset
    if target_width <= 1.0:
        raise ValueError("Right indent percent leaves no remaining preview width.")
    return QRectF(trimmed_rect.x(), trimmed_rect.y(), target_width, trimmed_rect.height())


def extract_qimage_region(image: QImage, rect: QRectF) -> QImage:
    if image.isNull():
        return QImage()
    normalized = rect.normalized()
    x = max(0, min(image.width() - 1, int(normalized.x())))
    y = max(0, min(image.height() - 1, int(normalized.y())))
    width = max(1, min(image.width() - x, int(round(normalized.width()))))
    height = max(1, min(image.height() - y, int(round(normalized.height()))))
    return image.copy(x, y, width, height)


class PdfDropFrame(QFrame):
    filesDropped = Signal(list)

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.setAcceptDrops(True)

    def dragEnterEvent(self, event) -> None:
        if extract_pdf_paths(event.mimeData()):
            event.acceptProposedAction()
            return
        event.ignore()

    def dragMoveEvent(self, event) -> None:
        if extract_pdf_paths(event.mimeData()):
            event.acceptProposedAction()
            return
        event.ignore()

    def dropEvent(self, event) -> None:
        paths = extract_pdf_paths(event.mimeData())
        if not paths:
            event.ignore()
            return
        self.filesDropped.emit(paths)
        event.acceptProposedAction()


class PreviewPane(QWidget):
    filesDropped = Signal(list)

    def __init__(self, parent: QWidget | None = None, *, align_top: bool = False) -> None:
        super().__init__(parent)
        self.image: QImage | None = None
        self.crop_rect: QRectF | None = None
        self.align_top = bool(align_top)
        self.setAcceptDrops(True)
        self.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)

    def set_preview(self, image: QImage | None, crop_rect: QRectF | None = None) -> None:
        self.image = QImage(image) if image is not None and not image.isNull() else None
        self.crop_rect = QRectF(crop_rect) if crop_rect is not None else None
        self.update()

    def clear_preview(self) -> None:
        self.image = None
        self.crop_rect = None
        self.update()

    def _draw_rect(self) -> QRectF:
        if self.image is None or self.image.isNull():
            return QRectF()
        margin = 8.0
        available_width = max(1.0, float(self.width()) - margin * 2.0)
        available_height = max(1.0, float(self.height()) - margin * 2.0)
        scale = min(
            available_width / max(float(self.image.width()), 1.0),
            available_height / max(float(self.image.height()), 1.0),
        )
        width = float(self.image.width()) * scale
        height = float(self.image.height()) * scale
        x = (self.width() - width) / 2.0
        y = margin if self.align_top else (self.height() - height) / 2.0
        return QRectF(x, y, width, height)

    def _image_rect_to_widget_rect(self, rect: QRectF) -> QRectF:
        draw_rect = self._draw_rect()
        if self.image is None or self.image.isNull() or draw_rect.isNull():
            return QRectF()
        scale_x = draw_rect.width() / max(float(self.image.width()), 1.0)
        scale_y = draw_rect.height() / max(float(self.image.height()), 1.0)
        return QRectF(
            draw_rect.x() + rect.x() * scale_x,
            draw_rect.y() + rect.y() * scale_y,
            rect.width() * scale_x,
            rect.height() * scale_y,
        )

    def dragEnterEvent(self, event) -> None:
        if extract_pdf_paths(event.mimeData()):
            event.acceptProposedAction()
            return
        event.ignore()

    def dragMoveEvent(self, event) -> None:
        if extract_pdf_paths(event.mimeData()):
            event.acceptProposedAction()
            return
        event.ignore()

    def dropEvent(self, event) -> None:
        paths = extract_pdf_paths(event.mimeData())
        if not paths:
            event.ignore()
            return
        self.filesDropped.emit(paths)
        event.acceptProposedAction()

    def paintEvent(self, _event) -> None:
        painter = QPainter(self)
        painter.fillRect(self.rect(), QColor(248, 248, 248))
        painter.setRenderHint(QPainter.Antialiasing, True)
        painter.setRenderHint(QPainter.SmoothPixmapTransform, True)

        if self.image is None or self.image.isNull():
            return

        draw_rect = self._draw_rect()
        painter.drawImage(draw_rect, self.image)

        if self.crop_rect is None or self.crop_rect.isNull():
            return

        crop_widget_rect = self._image_rect_to_widget_rect(self.crop_rect)
        if crop_widget_rect.isNull():
            return

        outer = QPainterPath()
        outer.addRect(draw_rect)
        inner = QPainterPath()
        inner.addRect(crop_widget_rect)
        painter.fillPath(outer.subtracted(inner), QColor(0, 0, 0, 70))
        painter.setPen(QPen(QColor(196, 196, 196), 2))
        painter.drawRect(crop_widget_rect)


class DelicateFileListWidget(QListWidget):
    filesDropped = Signal(list)
    fileRemoved = Signal()

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._hover_row = -1
        self.setAcceptDrops(True)
        self.setMouseTracking(True)
        self.setAlternatingRowColors(False)
        self.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self.setTextElideMode(Qt.ElideMiddle)
        self.setUniformItemSizes(True)
        self.setSizePolicy(QSizePolicy.Preferred, QSizePolicy.Expanding)

    def dragEnterEvent(self, event) -> None:
        if extract_pdf_paths(event.mimeData()):
            event.acceptProposedAction()
            return
        event.ignore()

    def dragMoveEvent(self, event) -> None:
        if extract_pdf_paths(event.mimeData()):
            event.acceptProposedAction()
            return
        event.ignore()

    def dropEvent(self, event) -> None:
        paths = extract_pdf_paths(event.mimeData())
        if not paths:
            event.ignore()
            return
        self.filesDropped.emit(paths)
        event.acceptProposedAction()

    def add_paths(self, paths: list[str]) -> bool:
        existing = {str(self.item(index).data(Qt.UserRole)) for index in range(self.count())}
        added_any = False
        for raw_path in paths:
            path = str(Path(raw_path).expanduser().resolve())
            if path in existing:
                continue
            item = QListWidgetItem(Path(path).name)
            item.setData(Qt.UserRole, path)
            item.setToolTip(path)
            self.addItem(item)
            added_any = True
        if added_any and self.currentRow() < 0:
            self.setCurrentRow(0)
        return added_any

    def selected_pdf_path(self) -> str | None:
        current_item = self.currentItem()
        if current_item is not None:
            return str(current_item.data(Qt.UserRole))
        if self.count() > 0:
            return str(self.item(0).data(Qt.UserRole))
        return None

    def _remove_rect_for_item(self, item: QListWidgetItem) -> QRectF:
        row_rect = self.visualItemRect(item)
        size = min(20, max(14, row_rect.height() - 8))
        return QRectF(row_rect.right() - size - 8, row_rect.center().y() - size / 2.0, size, size)

    def _hovered_item(self) -> QListWidgetItem | None:
        if self._hover_row < 0 or self._hover_row >= self.count():
            return None
        return self.item(self._hover_row)

    def _update_hover_row(self, pos) -> None:
        item = self.itemAt(pos)
        row = self.row(item) if item is not None else -1
        if row != self._hover_row:
            self._hover_row = row
            self.viewport().update()

    def mouseMoveEvent(self, event) -> None:
        self._update_hover_row(event.pos())
        super().mouseMoveEvent(event)

    def leaveEvent(self, event) -> None:
        self._hover_row = -1
        self.viewport().update()
        super().leaveEvent(event)

    def mousePressEvent(self, event) -> None:
        item = self.itemAt(event.pos())
        if item is not None:
            remove_rect = self._remove_rect_for_item(item)
            if remove_rect.contains(event.position()):
                row = self.row(item)
                self.takeItem(row)
                if self.count() > 0:
                    self.setCurrentRow(min(row, self.count() - 1))
                self.fileRemoved.emit()
                self.viewport().update()
                return
        super().mousePressEvent(event)

    def paintEvent(self, event) -> None:
        super().paintEvent(event)
        item = self._hovered_item()
        if item is None:
            return
        painter = QPainter(self.viewport())
        painter.setRenderHint(QPainter.Antialiasing, True)
        rect = self._remove_rect_for_item(item)
        painter.setPen(QPen(QColor(150, 150, 150), 1.6))
        painter.drawLine(rect.left(), rect.top(), rect.right(), rect.bottom())
        painter.drawLine(rect.right(), rect.top(), rect.left(), rect.bottom())


class ManualCropPreviewWidget(PreviewPane):
    cropChanged = Signal()

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._drag_mode = ""
        self._drag_start = QPointF()
        self._drag_origin_rect = QRectF()
        self._previous_rect = QRectF()

    def _handle_mode_for_widget_point(self, point: QPointF) -> str:
        if self.crop_rect is None or self.crop_rect.isNull():
            return ""
        crop_widget_rect = self._image_rect_to_widget_rect(self.crop_rect.normalized())
        if crop_widget_rect.isNull():
            return ""
        tolerance = 10.0
        near_left = abs(point.x() - crop_widget_rect.left()) <= tolerance
        near_right = abs(point.x() - crop_widget_rect.right()) <= tolerance
        near_top = abs(point.y() - crop_widget_rect.top()) <= tolerance
        near_bottom = abs(point.y() - crop_widget_rect.bottom()) <= tolerance
        within_x = crop_widget_rect.left() - tolerance <= point.x() <= crop_widget_rect.right() + tolerance
        within_y = crop_widget_rect.top() - tolerance <= point.y() <= crop_widget_rect.bottom() + tolerance
        if not within_x or not within_y:
            return ""
        if near_left and near_top:
            return "resize-top-left"
        if near_right and near_top:
            return "resize-top-right"
        if near_left and near_bottom:
            return "resize-bottom-left"
        if near_right and near_bottom:
            return "resize-bottom-right"
        if near_left:
            return "resize-left"
        if near_right:
            return "resize-right"
        if near_top:
            return "resize-top"
        if near_bottom:
            return "resize-bottom"
        return ""

    def _resized_rect(self, mode: str, image_point: QPointF) -> QRectF:
        if self.image is None or self.image.isNull():
            return QRectF()
        rect = QRectF(self._drag_origin_rect.normalized())
        min_width = 4.0
        min_height = 4.0
        max_x = float(self.image.width())
        max_y = float(self.image.height())
        x = min(max(image_point.x(), 0.0), max_x)
        y = min(max(image_point.y(), 0.0), max_y)

        if "left" in mode:
            rect.setLeft(min(max(0.0, x), rect.right() - min_width))
        if "right" in mode:
            rect.setRight(max(min(max_x, x), rect.left() + min_width))
        if "top" in mode:
            rect.setTop(min(max(0.0, y), rect.bottom() - min_height))
        if "bottom" in mode:
            rect.setBottom(max(min(max_y, y), rect.top() + min_height))
        return rect.normalized()

    def set_source_image(self, image: QImage | None, *, preserve_fractions: tuple[float, float, float, float] | None = None) -> None:
        if image is None or image.isNull():
            self.clear_preview()
            return
        if preserve_fractions is None:
            crop_rect = QRectF(0.0, 0.0, float(image.width()), float(image.height()))
        else:
            left, top, right, bottom = preserve_fractions
            crop_rect = QRectF(
                float(image.width()) * left,
                float(image.height()) * top,
                max(1.0, float(image.width()) * (1.0 - left - right)),
                max(1.0, float(image.height()) * (1.0 - top - bottom)),
            )
        self.set_preview(image, crop_rect)
        self.cropChanged.emit()

    def _widget_point_to_image_point(self, point: QPointF) -> QPointF | None:
        if self.image is None or self.image.isNull():
            return None
        draw_rect = self._draw_rect()
        if draw_rect.isNull() or not draw_rect.contains(point):
            return None
        scale_x = float(self.image.width()) / max(draw_rect.width(), 1.0)
        scale_y = float(self.image.height()) / max(draw_rect.height(), 1.0)
        x = (point.x() - draw_rect.x()) * scale_x
        y = (point.y() - draw_rect.y()) * scale_y
        x = min(max(x, 0.0), float(self.image.width()))
        y = min(max(y, 0.0), float(self.image.height()))
        return QPointF(x, y)

    def _clamp_rect(self, rect: QRectF) -> QRectF:
        if self.image is None or self.image.isNull():
            return rect
        max_x = float(self.image.width())
        max_y = float(self.image.height())
        width = min(rect.width(), max_x)
        height = min(rect.height(), max_y)
        x = min(max(rect.x(), 0.0), max_x - width)
        y = min(max(rect.y(), 0.0), max_y - height)
        return QRectF(x, y, width, height)

    def mousePressEvent(self, event) -> None:
        if event.button() != Qt.LeftButton:
            return
        image_point = self._widget_point_to_image_point(event.position())
        if image_point is None:
            return
        current_rect = self.crop_rect.normalized() if self.crop_rect is not None else QRectF()
        self._previous_rect = QRectF(current_rect)
        handle_mode = self._handle_mode_for_widget_point(event.position())
        if handle_mode:
            self._drag_mode = handle_mode
            self._drag_start = image_point
            self._drag_origin_rect = QRectF(current_rect)
            self.cropChanged.emit()
            self.update()
            return
        full_width = float(self.image.width()) if self.image is not None and not self.image.isNull() else 0.0
        full_height = float(self.image.height()) if self.image is not None and not self.image.isNull() else 0.0
        is_full_rect = (
            not current_rect.isNull()
            and abs(current_rect.width() - full_width) <= 1.0
            and abs(current_rect.height() - full_height) <= 1.0
            and abs(current_rect.x()) <= 1.0
            and abs(current_rect.y()) <= 1.0
        )
        if is_full_rect or current_rect.isNull() or not current_rect.contains(image_point):
            self._drag_mode = "draw"
            self._drag_start = image_point
            self._drag_origin_rect = QRectF(image_point, image_point).normalized()
            self.crop_rect = QRectF(image_point, image_point).normalized()
        else:
            self._drag_mode = ""
            return
        self.cropChanged.emit()
        self.update()

    def mouseMoveEvent(self, event) -> None:
        if not self._drag_mode:
            return
        image_point = self._widget_point_to_image_point(event.position())
        if image_point is None:
            return
        if self._drag_mode == "draw":
            self.crop_rect = QRectF(self._drag_start, image_point).normalized()
        else:
            self.crop_rect = self._resized_rect(self._drag_mode, image_point)
        self.cropChanged.emit()
        self.update()

    def mouseReleaseEvent(self, event) -> None:
        if event.button() != Qt.LeftButton or not self._drag_mode:
            return
        if self.image is not None and not self.image.isNull() and self.crop_rect is not None:
            if self.crop_rect.width() < 4.0 or self.crop_rect.height() < 4.0:
                if not self._previous_rect.isNull():
                    self.crop_rect = QRectF(self._previous_rect)
                else:
                    self.crop_rect = QRectF(0.0, 0.0, float(self.image.width()), float(self.image.height()))
        self._drag_mode = ""
        self.cropChanged.emit()
        self.update()

    def trim_fractions(self) -> tuple[float, float, float, float]:
        if self.image is None or self.image.isNull() or self.crop_rect is None or self.crop_rect.isNull():
            return (0.0, 0.0, 0.0, 0.0)
        image_width = float(self.image.width())
        image_height = float(self.image.height())
        rect = self.crop_rect.normalized()
        left = max(0.0, min(1.0, rect.x() / max(image_width, 1.0)))
        top = max(0.0, min(1.0, rect.y() / max(image_height, 1.0)))
        right = max(0.0, min(1.0, (image_width - (rect.x() + rect.width())) / max(image_width, 1.0)))
        bottom = max(0.0, min(1.0, (image_height - (rect.y() + rect.height())) / max(image_height, 1.0)))
        return (left, top, right, bottom)


class MainWindow(QMainWindow):
    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle(APP_NAME)
        self.resize(720, 620)

        self.cli_path = resolve_cli_path()
        self.python_path = resolve_python_path()
        self.process: QProcess | None = None
        self.stdout_buffer = ""
        self.pause_active = False
        self.active_operation = ""

        self.delicate_preview_cache: dict[str, dict[str, Any]] = {}
        self.manual_original_image: QImage | None = None
        self.manual_source_path = ""
        self.playboard_panel = load_playboard_module().PlayboardPanel()

        self.run_buttons: dict[str, QPushButton] = {}
        self.pause_buttons: dict[str, QPushButton] = {}
        self.stop_buttons: dict[str, QPushButton] = {}

        self.tabs = QTabWidget()

        self.overlap_source_input = QLineEdit()
        self.overlap_multiplier_input = self._build_field_input(DEFAULT_OVERLAP_MULTIPLIER)
        self.estimate_label = QLabel("Estimated width: -")
        self.overlap_progress_bar = self._build_progress_bar()
        self.overlap_progress_label = QLabel("0%")
        self.overlap_log = self._build_log_widget()

        self.crop_source_input = QLineEdit()
        self.crop_ratio_input = self._build_field_input(DEFAULT_CROP_RATIO)
        self.crop_progress_bar = self._build_progress_bar()
        self.crop_progress_label = QLabel("0%")
        self.crop_log = self._build_log_widget()

        self.delicate_file_list = DelicateFileListWidget()
        self.delicate_indent_input = self._build_field_input(DEFAULT_DELICATE_RIGHT_INDENT_PCT)
        self.delicate_preview = PreviewPane(align_top=True)
        self.manual_rotate_input = self._build_field_input("0")
        self.manual_save_button = QPushButton("Save")
        self.manual_preview = ManualCropPreviewWidget()

        self.convert_source_input = QLineEdit()
        self.convert_pdf_to_jpeg = QRadioButton("PDF to JPEG")
        self.convert_jpeg_to_pdf = QRadioButton("JPEG to PDF")
        self.convert_pdf_to_jpeg.setChecked(True)
        self.convert_progress_bar = self._build_progress_bar()
        self.convert_progress_label = QLabel("0%")
        self.convert_result_label = QLabel("Result: -")

        self.replace_cropped_input = QLineEdit()
        self.replace_target_input = QLineEdit()
        self.replace_onedrive_assisted = QCheckBox("ONEDRIVE ASSISTED REPLACE")
        self.replace_auto_freeup = QCheckBox("AUTO FREE-UP-SPACE")
        self.replace_auto_freeup.setChecked(True)
        self.replace_progress_bar = self._build_progress_bar()
        self.replace_progress_label = QLabel("0%")
        self.replace_log = self._build_log_widget()
        self.replace_step_labels: dict[str, QLabel] = {}
        self.progress_groups = {
            "overlap": (self.overlap_progress_bar, self.overlap_progress_label),
            "crop": (self.crop_progress_bar, self.crop_progress_label),
            "convert": (self.convert_progress_bar, self.convert_progress_label),
            "replace": (self.replace_progress_bar, self.replace_progress_label),
        }
        self.log_groups = {
            "overlap": self.overlap_log,
            "crop": self.crop_log,
            "replace": self.replace_log,
        }

        self._build_ui()
        self._wire_actions()

    def _build_field_input(self, value: str = "") -> QLineEdit:
        field = QLineEdit(value)
        field.setFixedWidth(FIELD_INPUT_WIDTH)
        return field

    def _build_progress_bar(self) -> QProgressBar:
        bar = QProgressBar()
        bar.setRange(0, 100)
        bar.setValue(0)
        bar.setTextVisible(False)
        bar.setStyleSheet(
            "QProgressBar {"
            "background: #ececec;"
            "border: 1px solid #cfcfcf;"
            "}"
            "QProgressBar::chunk {"
            "background: #8e8e8e;"
            "}"
        )
        return bar

    def _build_log_widget(self) -> QPlainTextEdit:
        widget = QPlainTextEdit()
        widget.setReadOnly(True)
        return widget

    def _tab_layout(self, widget: QWidget, *, spacing: int = 8) -> QVBoxLayout:
        layout = QVBoxLayout(widget)
        layout.setSpacing(spacing)
        return layout

    def _add_labeled_widget(self, layout: QVBoxLayout, label_text: str, widget: QWidget, *, stretch: int = 0) -> None:
        layout.addWidget(QLabel(label_text))
        layout.addWidget(widget, stretch)

    def _add_directory_section(self, layout: QVBoxLayout, label_text: str, line_edit: QLineEdit, title: str) -> None:
        self._add_labeled_widget(layout, label_text, self._directory_row(line_edit, title))

    def _build_ui(self) -> None:
        central = QWidget()
        self.setCentralWidget(central)
        root = QVBoxLayout(central)
        root.setContentsMargins(10, 10, 10, 10)
        root.setSpacing(10)
        root.addWidget(self.tabs, 1)

        self.tabs.addTab(self._build_overlap_tab(), "OVERLAP")
        self.tabs.addTab(self._build_crop_tab(), "CROP")
        self.tabs.addTab(self._build_delicate_tab(), "DELICATE CROP")
        self.tabs.addTab(self._build_playboard_tab(), "PLAYBOARD")
        self.tabs.addTab(self._build_convert_tab(), "CONVERT")
        self.tabs.addTab(self._build_replace_tab(), "REPLACE")

    def _directory_row(self, line_edit: QLineEdit, title: str) -> QWidget:
        wrapper = QWidget()
        row = QHBoxLayout(wrapper)
        row.setContentsMargins(0, 0, 0, 0)
        row.setSpacing(8)
        row.addWidget(line_edit, 1)
        button = QPushButton("Open")
        button.clicked.connect(lambda: self._pick_directory_into(line_edit, title))
        row.addWidget(button)
        return wrapper

    def _inline_field_row(self, label_text: str, field: QWidget, *, trailing: QWidget | None = None) -> QWidget:
        wrapper = QWidget()
        row = QHBoxLayout(wrapper)
        row.setContentsMargins(0, 0, 0, 0)
        row.setSpacing(8)
        label = QLabel(label_text)
        row.addWidget(label)
        row.addWidget(field)
        row.addStretch(1)
        if trailing is not None:
            row.addWidget(trailing)
        return wrapper

    def _action_row(self, mode: str) -> QWidget:
        wrapper = QWidget()
        row = QHBoxLayout(wrapper)
        row.setContentsMargins(0, 0, 0, 0)
        row.setSpacing(8)
        run_button = QPushButton("RUN")
        pause_button = QPushButton("PAUSE")
        stop_button = QPushButton("STOP")
        run_button.clicked.connect(self.run_current_mode)
        pause_button.clicked.connect(self.toggle_pause)
        stop_button.clicked.connect(self.stop_process)
        self.run_buttons[mode] = run_button
        self.pause_buttons[mode] = pause_button
        self.stop_buttons[mode] = stop_button
        row.addWidget(run_button)
        row.addWidget(pause_button)
        row.addWidget(stop_button)
        row.addStretch(1)
        return wrapper

    def _add_progress_row(self, layout: QVBoxLayout, progress_bar: QProgressBar, progress_label: QLabel) -> None:
        row = QHBoxLayout()
        row.setContentsMargins(0, 0, 0, 0)
        row.setSpacing(8)
        row.addWidget(progress_bar, 1)
        row.addWidget(progress_label)
        layout.addLayout(row)

    def _build_replace_steps_row(self) -> QWidget:
        wrapper = QWidget()
        row = QHBoxLayout(wrapper)
        row.setContentsMargins(0, 0, 0, 0)
        row.setSpacing(6)
        for step, text in REPLACE_STEP_LABELS.items():
            label = QLabel(text)
            label.setAlignment(Qt.AlignCenter)
            label.setFrameShape(QFrame.StyledPanel)
            label.setStyleSheet("QLabel { background: #ececec; border: 1px solid #cfcfcf; padding: 4px 6px; }")
            self.replace_step_labels[step] = label
            row.addWidget(label, 1)
        return wrapper

    def set_replace_step_state(self, step: str, state: str) -> None:
        label = self.replace_step_labels.get(step)
        if label is None:
            return
        styles = {
            "pending": "QLabel { background: #ececec; border: 1px solid #cfcfcf; padding: 4px 6px; color: #666666; }",
            "active": "QLabel { background: #dcdcdc; border: 1px solid #b5b5b5; padding: 4px 6px; color: #111111; }",
            "done": "QLabel { background: #c9c9c9; border: 1px solid #9b9b9b; padding: 4px 6px; color: #111111; }",
            "skipped": "QLabel { background: #f4f4f4; border: 1px solid #d7d7d7; padding: 4px 6px; color: #8a8a8a; }",
            "failed": "QLabel { background: #efdddd; border: 1px solid #caa6a6; padding: 4px 6px; color: #7a2a2a; }",
        }
        label.setStyleSheet(styles.get(state, styles["pending"]))

    def reset_replace_step_states(self) -> None:
        for step in REPLACE_STEP_LABELS:
            self.set_replace_step_state(step, "pending")

    def _build_overlap_tab(self) -> QWidget:
        widget = QWidget()
        layout = self._tab_layout(widget)
        self._add_directory_section(layout, "Source", self.overlap_source_input, "Select Overlap Source Directory")
        self._add_labeled_widget(layout, "Multiplier", self.overlap_multiplier_input)
        layout.addWidget(self._action_row("overlap"))
        layout.addWidget(self.estimate_label)
        self._add_progress_row(layout, self.overlap_progress_bar, self.overlap_progress_label)
        layout.addWidget(self.overlap_log, 1)
        return widget

    def _build_crop_tab(self) -> QWidget:
        widget = QWidget()
        layout = self._tab_layout(widget)
        self._add_directory_section(layout, "Source", self.crop_source_input, "Select Crop Source Directory")
        self._add_labeled_widget(layout, "Ratio", self.crop_ratio_input)
        layout.addWidget(self._action_row("crop"))
        self._add_progress_row(layout, self.crop_progress_bar, self.crop_progress_label)
        layout.addWidget(self.crop_log, 1)
        return widget

    def _build_delicate_tab(self) -> QWidget:
        widget = QWidget()
        layout = QVBoxLayout(widget)
        layout.setSpacing(10)
        layout.addWidget(self._action_row("delicate-crop"))

        batch_frame = QFrame()
        batch_frame.setFrameShape(QFrame.StyledPanel)
        batch_layout = QVBoxLayout(batch_frame)
        batch_layout.setContentsMargins(8, 8, 8, 8)
        batch_layout.setSpacing(8)
        batch_layout.addWidget(self._inline_field_row("Right Indent %", self.delicate_indent_input))
        batch_body = QHBoxLayout()
        batch_body.setContentsMargins(0, 0, 0, 0)
        batch_body.setSpacing(8)
        batch_body.addWidget(self.delicate_file_list, 1)
        batch_body.addWidget(self.delicate_preview, 1)
        batch_body.setStretch(0, 2)
        batch_body.setStretch(1, 3)
        batch_layout.addLayout(batch_body, 1)

        manual_frame = PdfDropFrame()
        manual_frame.setFrameShape(QFrame.StyledPanel)
        manual_frame.filesDropped.connect(self.handle_manual_drop)
        manual_layout = QVBoxLayout(manual_frame)
        manual_layout.setContentsMargins(8, 8, 8, 8)
        manual_layout.setSpacing(8)
        manual_top_row = QHBoxLayout()
        manual_top_row.setContentsMargins(0, 0, 0, 0)
        manual_top_row.setSpacing(8)
        manual_top_row.addWidget(QLabel("Rotate"))
        manual_top_row.addWidget(self.manual_rotate_input)
        manual_top_row.addStretch(1)
        manual_top_row.addWidget(self.manual_save_button)
        manual_layout.addLayout(manual_top_row)
        manual_layout.addWidget(self.manual_preview, 1)

        layout.addWidget(batch_frame, 1)
        layout.addWidget(manual_frame, 1)
        return widget

    def _build_convert_tab(self) -> QWidget:
        widget = QWidget()
        layout = self._tab_layout(widget)
        self._add_directory_section(layout, "Source", self.convert_source_input, "Select Convert Source Directory")
        mode_row = QHBoxLayout()
        mode_row.setContentsMargins(0, 0, 0, 0)
        mode_row.setSpacing(12)
        mode_row.addWidget(self.convert_pdf_to_jpeg)
        mode_row.addWidget(self.convert_jpeg_to_pdf)
        mode_row.addStretch(1)
        layout.addLayout(mode_row)
        layout.addWidget(self._action_row("convert"))
        self._add_progress_row(layout, self.convert_progress_bar, self.convert_progress_label)
        layout.addWidget(self.convert_result_label)
        layout.addStretch(1)
        return widget

    def _build_playboard_tab(self) -> QWidget:
        return self.playboard_panel

    def _build_replace_tab(self) -> QWidget:
        widget = QWidget()
        layout = self._tab_layout(widget)
        self._add_directory_section(layout, "Source", self.replace_cropped_input, "Select Cropped Directory")
        self._add_directory_section(layout, "Destination", self.replace_target_input, "Select Replacement Directory")
        option_row = QHBoxLayout()
        option_row.setContentsMargins(0, 0, 0, 0)
        option_row.setSpacing(12)
        option_row.addWidget(self.replace_onedrive_assisted)
        option_row.addWidget(self.replace_auto_freeup)
        option_row.addStretch(1)
        layout.addLayout(option_row)
        layout.addWidget(self._action_row("replace"))
        layout.addWidget(self._build_replace_steps_row())
        self._add_progress_row(layout, self.replace_progress_bar, self.replace_progress_label)
        layout.addWidget(self.replace_log, 1)
        return widget

    def _wire_actions(self) -> None:
        self.delicate_file_list.currentItemChanged.connect(self.update_delicate_preview)
        self.delicate_file_list.filesDropped.connect(self.handle_batch_drop)
        self.delicate_file_list.fileRemoved.connect(self.update_delicate_preview)
        self.delicate_indent_input.textChanged.connect(self.update_delicate_preview)
        self.manual_rotate_input.textChanged.connect(self.on_manual_rotation_text_changed)
        self.manual_save_button.clicked.connect(self.run_manual_crop)
        self.manual_preview.filesDropped.connect(self.handle_manual_drop)
        self.replace_onedrive_assisted.toggled.connect(self.update_replace_option_controls)
        self.update_replace_option_controls()

    def current_mode(self) -> str:
        return TAB_MODE_BY_LABEL[self.tabs.tabText(self.tabs.currentIndex())]

    def current_output_group(self) -> str:
        if self.active_operation in OUTPUT_GROUP_BY_OPERATION:
            return OUTPUT_GROUP_BY_OPERATION[self.active_operation]
        mode = self.current_mode()
        return "delicate" if mode == "delicate-crop" else mode

    def reset_group_output(self, group: str) -> None:
        progress_widgets = self.progress_groups.get(group)
        if progress_widgets is not None:
            progress_bar, progress_label = progress_widgets
            progress_bar.setRange(0, 100)
            progress_bar.setValue(0)
            progress_label.setText("0%")

        log_widget = self.log_groups.get(group)
        if log_widget is not None:
            log_widget.clear()

        if group == "convert":
            self.convert_result_label.setText("Result: -")
        elif group == "replace":
            self.reset_replace_step_states()

    def remove_delicate_paths(self, processed_paths: list[str]) -> None:
        if not processed_paths:
            return
        target_set = {str(Path(path).expanduser().resolve()) for path in processed_paths}
        for index in range(self.delicate_file_list.count() - 1, -1, -1):
            item = self.delicate_file_list.item(index)
            item_path = str(Path(str(item.data(Qt.UserRole))).expanduser().resolve())
            if item_path in target_set:
                self.delicate_file_list.takeItem(index)
        if self.delicate_file_list.count() > 0:
            self.delicate_file_list.setCurrentRow(0)
        else:
            self.delicate_preview.clear_preview()

    def clear_manual_crop_state(self) -> None:
        self.manual_source_path = ""
        self.manual_original_image = None
        self.manual_preview.clear_preview()
        self.manual_rotate_input.setText("0")

    def append_log(self, message: str, *, group: str | None = None) -> None:
        target_group = group or self.current_output_group()
        widget = self.log_groups.get(target_group)
        if widget is not None and message:
            widget.appendPlainText(message)

    def update_replace_option_controls(self) -> None:
        windows_supported = sys.platform.startswith("win")
        if not windows_supported:
            self.replace_onedrive_assisted.setChecked(False)
            self.replace_onedrive_assisted.setEnabled(False)
            self.replace_auto_freeup.setEnabled(False)
        else:
            self.replace_onedrive_assisted.setEnabled(True)
            self.replace_auto_freeup.setEnabled(self.replace_onedrive_assisted.isChecked())

    def _show_error(self, message: str) -> None:
        QMessageBox.critical(self, APP_NAME, message)

    def _show_warning(self, message: str) -> None:
        QMessageBox.warning(self, APP_NAME, message)

    def set_pause_button_labels(self, text: str) -> None:
        for button in self.pause_buttons.values():
            button.setText(text)

    def _pick_directory_into(self, target: QLineEdit, title: str) -> None:
        path = QFileDialog.getExistingDirectory(self, title)
        if path:
            target.setText(path)

    def handle_batch_drop(self, paths: list[str]) -> None:
        added_any = self.delicate_file_list.add_paths(paths)
        if added_any:
            self.update_delicate_preview()

    def handle_manual_drop(self, paths: list[str]) -> None:
        if not paths:
            return
        self.load_manual_source_preview(str(paths[0]))

    def _cached_preview_bundle(self, pdf_path: str | Path, *, dpi: int, max_width: int) -> dict[str, Any]:
        resolved = str(Path(pdf_path).expanduser().resolve())
        mtime_ns = Path(resolved).stat().st_mtime_ns
        cached = self.delicate_preview_cache.get(resolved)
        if cached is not None and int(cached.get("mtime_ns", -1)) == mtime_ns:
            return cached
        preview_bundle = load_pdf_preview_bundle(resolved, dpi=dpi, max_width=max_width)
        self.delicate_preview_cache[resolved] = preview_bundle
        return preview_bundle

    def update_delicate_preview(self, *_args) -> None:
        selected_pdf = self.delicate_file_list.selected_pdf_path()
        if not selected_pdf:
            self.delicate_preview.clear_preview()
            return
        try:
            preview_bundle = self._cached_preview_bundle(selected_pdf, dpi=96, max_width=760)
            indent_pct = parse_float(
                self.delicate_indent_input.text() or DEFAULT_DELICATE_RIGHT_INDENT_PCT,
                "Right indent percent",
                minimum=0.0,
                maximum=99.999,
            )
            trimmed_image = extract_qimage_region(preview_bundle["image"], preview_bundle["trimmed_rect"])
            if trimmed_image.isNull():
                raise ValueError("Failed to extract delicate preview image.")
            display_rect = QRectF(0.0, 0.0, float(trimmed_image.width()), float(trimmed_image.height()))
            crop_rect = compute_right_indented_image_rect(display_rect, indent_pct)
        except Exception:
            self.delicate_preview.clear_preview()
            return
        self.delicate_preview.set_preview(trimmed_image, crop_rect)

    def load_manual_source_preview(self, pdf_path: str) -> None:
        try:
            preview_bundle = load_pdf_preview_bundle(pdf_path, dpi=120, max_width=900)
        except Exception as exc:
            self.manual_original_image = None
            self.manual_source_path = ""
            self.manual_preview.clear_preview()
            self._show_error(str(exc))
            return
        self.manual_source_path = str(preview_bundle["pdf_path"])
        self.delicate_preview_cache[str(preview_bundle["pdf_path"])] = preview_bundle
        self.manual_original_image = preview_bundle["image"]
        self.update_manual_preview(reset_crop=True)

    def _manual_rotation_value(self, *, allow_partial: bool = False) -> float | None:
        text = (self.manual_rotate_input.text() or "").strip()
        if allow_partial and text in {"", "-", "+", ".", "-.", "+."}:
            return None
        return parse_float(text or "0", "Rotate")

    def on_manual_rotation_text_changed(self) -> None:
        try:
            value = self._manual_rotation_value(allow_partial=True)
        except Exception:
            return
        if value is None:
            return
        self.update_manual_preview(reset_crop=False)

    def update_manual_preview(self, *, reset_crop: bool) -> None:
        if self.manual_original_image is None or self.manual_original_image.isNull():
            self.manual_preview.clear_preview()
            return
        try:
            rotation = self._manual_rotation_value() or 0.0
        except Exception:
            rotation = 0.0
        preserve = None if reset_crop else self.manual_preview.trim_fractions()
        transform = QTransform()
        transform.rotate(-rotation)
        rotated = self.manual_original_image.transformed(transform, Qt.SmoothTransformation)
        self.manual_preview.set_source_image(rotated, preserve_fractions=preserve)

    def _required_text(self, line_edit: QLineEdit, missing_message: str) -> str:
        value = line_edit.text().strip()
        if not value:
            raise ValueError(missing_message)
        return value

    def _list_widget_paths(self, widget: QListWidget) -> list[str]:
        return [str(widget.item(index).data(Qt.UserRole)) for index in range(widget.count())]

    def _build_overlap_cli_arguments(self) -> list[str]:
        source = self._required_text(self.overlap_source_input, "Select an overlap source directory.")
        return [
            "overlap",
            "--source-dir",
            source,
            "--overlap-multiplier",
            self.overlap_multiplier_input.text().strip() or DEFAULT_OVERLAP_MULTIPLIER,
            "--export-csv",
            "--export-overlap",
            "--export-extracted-original",
        ]

    def _build_crop_cli_arguments(self) -> list[str]:
        source = self._required_text(self.crop_source_input, "Select a crop source directory.")
        return [
            "crop",
            "--source-dir",
            source,
            "--crop-ratio",
            self.crop_ratio_input.text().strip() or DEFAULT_CROP_RATIO,
        ]

    def _build_delicate_cli_arguments(self) -> list[str]:
        file_paths = self._list_widget_paths(self.delicate_file_list)
        if not file_paths:
            raise ValueError("Select at least one PDF for delicate crop.")
        indent_pct = parse_float(
            self.delicate_indent_input.text() or DEFAULT_DELICATE_RIGHT_INDENT_PCT,
            "Right indent percent",
            minimum=0.0,
            maximum=99.999,
        )
        arguments = ["delicate-crop"]
        for path in file_paths:
            arguments.extend(["--file-path", path])
        arguments.extend(["--right-indent-pct", str(indent_pct)])
        return arguments

    def _build_playboard_cli_arguments(self) -> list[str]:
        raise ValueError("Playboard does not use run/pause/stop.")

    def _build_convert_cli_arguments(self) -> list[str]:
        source = self._required_text(self.convert_source_input, "Select a convert source directory.")
        convert_mode = "pdf-to-jpeg" if self.convert_pdf_to_jpeg.isChecked() else "jpeg-to-pdf"
        return [convert_mode, "--source-dir", source]

    def _build_replace_cli_arguments(self) -> list[str]:
        cropped_dir = self._required_text(self.replace_cropped_input, "Select a replace source directory.")
        replacement_dir = self._required_text(self.replace_target_input, "Select a replace destination directory.")
        arguments = [
            "replace",
            "--cropped-dir",
            cropped_dir,
            "--replacement-dir",
            replacement_dir,
        ]
        if self.replace_onedrive_assisted.isChecked():
            arguments.append("--onedrive-assisted")
            if not self.replace_auto_freeup.isChecked():
                arguments.append("--no-auto-freeup")
        return arguments

    def build_cli_arguments(self) -> list[str]:
        mode = self.current_mode()
        builders = {
            "overlap": self._build_overlap_cli_arguments,
            "crop": self._build_crop_cli_arguments,
            "delicate-crop": self._build_delicate_cli_arguments,
            "playboard": self._build_playboard_cli_arguments,
            "convert": self._build_convert_cli_arguments,
            "replace": self._build_replace_cli_arguments,
        }
        return builders[mode]()

    def start_process(self, cli_arguments: list[str]) -> None:
        if self.process and self.process.state() != QProcess.NotRunning:
            self._show_warning("A task is already running.")
            return
        if not self.cli_path.exists():
            self._show_error(f"Missing CLI: {self.cli_path}")
            return
        if not cli_arguments:
            self._show_error("No operation specified.")
            return

        self.active_operation = str(cli_arguments[0])
        output_group = self.current_output_group()
        self.reset_group_output(output_group)
        self.pause_active = False
        self.set_pause_button_labels("PAUSE")
        self.stdout_buffer = ""

        self.process = QProcess(self)
        self.process.setProgram(str(self.python_path))
        if getattr(sys, "frozen", False):
            process_arguments = cli_arguments
        else:
            process_arguments = [str(self.cli_path), *cli_arguments]
        self.process.setArguments(process_arguments)
        self.process.setProcessChannelMode(QProcess.MergedChannels)
        self.process.readyReadStandardOutput.connect(self.read_process_output)
        self.process.finished.connect(self.handle_process_finished)
        self.process.start()

    def run_current_mode(self) -> None:
        try:
            arguments = self.build_cli_arguments()
        except Exception as exc:
            self._show_error(str(exc))
            return
        self.start_process(arguments)

    def run_manual_crop(self) -> None:
        source_pdf = self.manual_source_path.strip()
        if not source_pdf:
            self._show_error("Drop a PDF into the manual crop area first.")
            return
        if self.manual_original_image is None or self.manual_original_image.isNull():
            self._show_error("Load a PDF preview before saving.")
            return
        try:
            rotation = self._manual_rotation_value() or 0.0
        except Exception as exc:
            self._show_error(str(exc))
            return

        backend = load_backend_module()
        output_paths = backend.resolve_manual_output_paths(Path(source_pdf))
        left, top, right, bottom = self.manual_preview.trim_fractions()
        arguments = [
            "manual-crop",
            "--source-pdf",
            source_pdf,
            "--output-pdf",
            str(output_paths["mcropped_path"]),
            "--archive-original-to",
            str(output_paths["original_save_path"]),
            "--rotate-degrees",
            str(rotation),
            "--trim-left",
            str(left),
            "--trim-top",
            str(top),
            "--trim-right",
            str(right),
            "--trim-bottom",
            str(bottom),
        ]
        self.start_process(arguments)

    def read_process_output(self) -> None:
        if not self.process:
            return
        chunk = bytes(self.process.readAllStandardOutput()).decode("utf-8", errors="replace")
        if not chunk:
            return
        self.stdout_buffer += chunk
        while "\n" in self.stdout_buffer:
            line, self.stdout_buffer = self.stdout_buffer.split("\n", 1)
            line = line.rstrip()
            if line:
                self.handle_process_line(line)

    def _set_group_progress(self, group: str, percent: int) -> None:
        progress_widgets = self.progress_groups.get(group)
        if progress_widgets is None:
            return
        progress_bar, progress_label = progress_widgets
        progress_bar.setRange(0, 100)
        progress_bar.setValue(percent)
        progress_label.setText(f"{percent}%")

    def _handle_result_payload(self, payload: dict[str, Any]) -> None:
        mode = str(payload.get("mode", "task"))
        if not bool(payload.get("ok")):
            return
        if mode == "delicate-crop":
            self.remove_delicate_paths([str(path) for path in payload.get("updated_paths", [])])
            if self.delicate_file_list.count() == 0:
                self.delicate_preview.clear_preview()
            return
        if mode == "manual-crop":
            self.clear_manual_crop_state()
            return
        if mode in {"pdf-to-jpeg", "jpeg-to-pdf"}:
            created_count = int(payload.get("created_count", 0))
            error_count = int(payload.get("error_count", 0))
            self.convert_result_label.setText(f"Result: created {created_count}, errors {error_count}")
            return
        if mode == "replace":
            result_csv_path = str(payload.get("result_csv_path", "")).strip()
            if result_csv_path:
                self.append_log(f"Result CSV: {result_csv_path}", group="replace")

    def _handle_log_event(self, payload: dict[str, Any], group: str) -> None:
        self.append_log(str(payload.get("message", "")), group=group)

    def _handle_progress_event(self, payload: dict[str, Any], group: str) -> None:
        done = int(payload.get("done", 0))
        total = max(int(payload.get("total", 0)), 0)
        percent = 0 if total <= 0 else int(round((done / total) * 100.0))
        self._set_group_progress(group, max(0, min(percent, 100)))

    def _handle_estimate_event(self, payload: dict[str, Any], _group: str) -> None:
        self.estimate_label.setText(str(payload.get("text", "Estimated width: -")))

    def _handle_suggested_cropped_dir_event(self, payload: dict[str, Any], _group: str) -> None:
        self.replace_cropped_input.setText(str(payload.get("path", "")))

    def _handle_replace_step_event(self, payload: dict[str, Any], _group: str) -> None:
        self.set_replace_step_state(str(payload.get("step", "")), str(payload.get("state", "pending")))

    def _handle_error_event(self, payload: dict[str, Any], group: str) -> None:
        message = str(payload.get("message", "Unknown error"))
        self.append_log(message, group=group)
        if self.log_groups.get(group) is None:
            self._show_error(message)

    def handle_process_line(self, line: str) -> None:
        try:
            payload = json.loads(line)
        except Exception:
            self.append_log(line)
            return

        group = self.current_output_group()
        event = payload.get("event")
        handlers = {
            "log": self._handle_log_event,
            "progress": self._handle_progress_event,
            "estimate": self._handle_estimate_event,
            "suggested-cropped-dir": self._handle_suggested_cropped_dir_event,
            "replace-step": self._handle_replace_step_event,
            "error": self._handle_error_event,
        }
        handler = handlers.get(str(event))
        if handler is not None:
            handler(payload, group)
            return
        if event == "result":
            self._handle_result_payload(payload)

    def handle_process_finished(self, exit_code: int, _exit_status) -> None:
        if self.stdout_buffer.strip():
            self.handle_process_line(self.stdout_buffer.strip())
        self.stdout_buffer = ""
        if exit_code != 0:
            group = self.current_output_group()
            self.append_log(f"Exited with code {exit_code}", group=group)
        self.pause_active = False
        self.set_pause_button_labels("PAUSE")
        self.active_operation = ""

    def send_control(self, command: str) -> None:
        if not self.process or self.process.state() == QProcess.NotRunning:
            return
        self.process.write((command + "\n").encode("utf-8"))

    def toggle_pause(self) -> None:
        if not self.process or self.process.state() == QProcess.NotRunning:
            return
        if self.pause_active:
            self.send_control("resume")
            self.pause_active = False
            self.set_pause_button_labels("PAUSE")
        else:
            self.send_control("pause")
            self.pause_active = True
            self.set_pause_button_labels("RESUME")

    def stop_process(self) -> None:
        if not self.process or self.process.state() == QProcess.NotRunning:
            return
        self.send_control("stop")
        self.pause_active = False
        self.set_pause_button_labels("PAUSE")

    def closeEvent(self, event) -> None:
        if self.process and self.process.state() != QProcess.NotRunning:
            answer = QMessageBox.question(self, APP_NAME, "A task is still running. Stop it and close?")
            if answer != QMessageBox.Yes:
                event.ignore()
                return
            self.stop_process()
            self.process.waitForFinished(2000)
        event.accept()


def main() -> None:
    if len(sys.argv) > 1 and sys.argv[1] in CLI_MODES:
        raise SystemExit(run_cli_main())
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    raise SystemExit(app.exec())


if __name__ == "__main__":
    main()
