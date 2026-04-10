#!/usr/bin/env python3
from __future__ import annotations

import importlib.util
import json
import os
import queue
import sys
import threading
import traceback
from pathlib import Path
from typing import Any, Optional

from PySide6.QtCore import QTimer, Qt
from PySide6.QtGui import QColor, QFont, QFontDatabase
from PySide6.QtWidgets import (
    QApplication,
    QFileDialog,
    QFrame,
    QGraphicsDropShadowEffect,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMessageBox,
    QPlainTextEdit,
    QProgressBar,
    QPushButton,
    QSizePolicy,
    QStackedWidget,
    QVBoxLayout,
    QWidget,
)


APP_NAME = "Microfiche Preprocess"
GLOBAL_GUTTER = 32
SIDEBAR_WIDTH = 220
FIELD_HEIGHT = 32
BUTTON_HEIGHT = 32
PANEL_RADIUS = 18
SHADOW_BLUR = 28
SHADOW_ALPHA = 24


def _resource_root() -> Path:
    if getattr(sys, "frozen", False):
        meipass = getattr(sys, "_MEIPASS", None)
        if meipass:
            return Path(meipass)
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


def _load_module(file_name: str, module_name: str):
    script_path = _resource_root() / file_name
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Failed to load {script_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


_BACKEND = _load_module("microfiche-preprocess.py", "microfiche_preprocess_backend")

DEFAULT_CROP_RATIO = _BACKEND.DEFAULT_CROP_RATIO
PY_WIDTH_OVERLAP_REL_THRESHOLD = _BACKEND.PY_WIDTH_OVERLAP_REL_THRESHOLD
CropRunConfig = _BACKEND.CropRunConfig
JpegToPdfRunConfig = _BACKEND.JpegToPdfRunConfig
OverlapRunConfig = _BACKEND.OverlapRunConfig
PdfToJpegRunConfig = _BACKEND.PdfToJpegRunConfig
PipelineController = _BACKEND.PipelineController
PipelineHooks = _BACKEND.PipelineHooks
ReplaceRunConfig = _BACKEND.ReplaceRunConfig
Storage = _BACKEND.Storage
batch_root_for_path = _BACKEND.batch_root_for_path
ensure_memory_schema = _BACKEND.ensure_memory_schema
run_crop_pipeline = _BACKEND.run_crop_pipeline
run_jpeg_to_pdf_pipeline = _BACKEND.run_jpeg_to_pdf_pipeline
run_overlap_pipeline = _BACKEND.run_overlap_pipeline
run_pdf_to_jpeg_pipeline = _BACKEND.run_pdf_to_jpeg_pipeline
run_replace_pipeline = _BACKEND.run_replace_pipeline


def _load_app_font() -> str:
    candidates = [
        _resource_root() / "fonts",
        _resource_root().parent / "fonts",
        _resource_root().parent.parent / "fonts",
    ]
    family = ""
    for directory in candidates:
        if not directory.exists():
            continue
        for font_path in sorted(directory.glob("*.ttf")):
            font_id = QFontDatabase.addApplicationFont(str(font_path))
            if font_id >= 0:
                families = QFontDatabase.applicationFontFamilies(font_id)
                if families and not family:
                    family = families[0]
    return family or QFont().defaultFamily()


APP_FONT_FAMILY = ""


def _make_font(size: int, weight: int = QFont.Weight.Medium) -> QFont:
    font = QFont(APP_FONT_FAMILY)
    font.setPointSize(size)
    font.setWeight(weight)
    return font


def _shadow_effect() -> QGraphicsDropShadowEffect:
    effect = QGraphicsDropShadowEffect()
    effect.setBlurRadius(SHADOW_BLUR)
    effect.setOffset(0, 10)
    effect.setColor(QColor(0, 0, 0, SHADOW_ALPHA))
    return effect


class SurfaceFrame(QFrame):
    def __init__(self, class_name: str = "surface", selectable: bool = False) -> None:
        super().__init__()
        self.setObjectName(class_name)
        self.setProperty("selectableSurface", selectable)
        self.setProperty("activeSurface", False)
        self.setGraphicsEffect(_shadow_effect())

    def set_active(self, active: bool) -> None:
        self.setProperty("activeSurface", active)
        self.style().unpolish(self)
        self.style().polish(self)
        self.update()


class SidebarButton(QPushButton):
    def __init__(self, text: str) -> None:
        super().__init__(text)
        self.setCheckable(True)
        self.setCursor(Qt.CursorShape.PointingHandCursor)
        self.setFixedHeight(BUTTON_HEIGHT)
        self.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)
        self.setProperty("sidebarButton", True)


class ActionButton(QPushButton):
    def __init__(self, text: str, emphasized: bool = False) -> None:
        super().__init__(text)
        self.setCursor(Qt.CursorShape.PointingHandCursor)
        self.setFixedHeight(BUTTON_HEIGHT)
        self.setProperty("actionButton", True)
        self.setProperty("emphasized", emphasized)


class SectionTitle(QLabel):
    def __init__(self, text: str) -> None:
        super().__init__(text)
        self.setProperty("sectionTitle", True)


class FieldLabel(QLabel):
    def __init__(self, text: str, width: int = 116) -> None:
        super().__init__(text.upper())
        self.setProperty("fieldLabel", True)
        self.setFixedWidth(width)
        self.setAlignment(Qt.AlignmentFlag.AlignVCenter | Qt.AlignmentFlag.AlignLeft)


class TextField(QLineEdit):
    def __init__(self, text: str = "") -> None:
        super().__init__(text)
        self.setProperty("formField", True)
        self.setFixedHeight(FIELD_HEIGHT)


class BrowseButton(QPushButton):
    def __init__(self, text: str = "OPEN") -> None:
        super().__init__(text)
        self.setCursor(Qt.CursorShape.PointingHandCursor)
        self.setProperty("browseButton", True)
        self.setFixedHeight(FIELD_HEIGHT)
        self.setFixedWidth(88)


class DirectoryRow(QWidget):
    def __init__(self, label: str, button_text: str = "OPEN") -> None:
        super().__init__()
        layout = QHBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(GLOBAL_GUTTER)
        self.label = FieldLabel(label)
        self.field = TextField("")
        self.button = BrowseButton(button_text)
        layout.addWidget(self.label)
        layout.addWidget(self.field, 1)
        layout.addWidget(self.button)

    def text(self) -> str:
        return self.field.text().strip()

    def set_text(self, value: str) -> None:
        self.field.setText(value)


class NumberRow(QWidget):
    def __init__(self, label: str, value: str) -> None:
        super().__init__()
        layout = QHBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(GLOBAL_GUTTER)
        self.label = FieldLabel(label)
        self.field = TextField(value)
        self.field.setAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
        layout.addWidget(self.label)
        layout.addWidget(self.field, 1)

    def text(self) -> str:
        return self.field.text().strip()

    def set_text(self, value: str) -> None:
        self.field.setText(value)


class ConvertPanel(SurfaceFrame):
    def __init__(self, title: str, action_name: str) -> None:
        super().__init__("surface", selectable=True)
        self.action_name = action_name
        layout = QVBoxLayout(self)
        layout.setContentsMargins(GLOBAL_GUTTER, GLOBAL_GUTTER, GLOBAL_GUTTER, GLOBAL_GUTTER)
        layout.setSpacing(GLOBAL_GUTTER)
        self.button = QPushButton(title)
        self.button.setCursor(Qt.CursorShape.PointingHandCursor)
        self.button.setProperty("convertSelector", True)
        self.button.setFixedHeight(BUTTON_HEIGHT)
        self.button.setSizePolicy(QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Fixed)
        self.directory_row = DirectoryRow("Source")
        layout.addWidget(self.button, 0, Qt.AlignmentFlag.AlignLeft)
        layout.addWidget(self.directory_row)

    def mousePressEvent(self, event) -> None:  # type: ignore[override]
        self.button.click()
        super().mousePressEvent(event)


class FrontendWindow(QMainWindow):
    def __init__(self) -> None:
        super().__init__()
        self.current_mode = "overlap"
        self.current_convert_action = "pdf-to-jpeg"
        self.worker_thread: Optional[threading.Thread] = None
        self.cancel_event = threading.Event()
        self.pause_event = threading.Event()
        self.event_queue: queue.Queue[tuple[str, dict[str, Any]]] = queue.Queue()
        self.progress_total = 1
        self.progress_done = 0

        self.setWindowTitle(APP_NAME)
        self.resize(1120, 820)
        self.setMinimumSize(980, 720)

        self._build_ui()
        self._apply_styles()
        self._wire_events()
        self._refresh_mode()
        self._refresh_convert_action()

        self.drain_timer = QTimer(self)
        self.drain_timer.setInterval(80)
        self.drain_timer.timeout.connect(self._drain_events)
        self.drain_timer.start()

    def _build_ui(self) -> None:
        root = QWidget()
        root_layout = QHBoxLayout(root)
        root_layout.setContentsMargins(GLOBAL_GUTTER, GLOBAL_GUTTER, GLOBAL_GUTTER, GLOBAL_GUTTER)
        root_layout.setSpacing(GLOBAL_GUTTER)

        self.sidebar = SurfaceFrame("sidebar")
        self.sidebar.setFixedWidth(SIDEBAR_WIDTH)
        sidebar_layout = QVBoxLayout(self.sidebar)
        sidebar_layout.setContentsMargins(GLOBAL_GUTTER, GLOBAL_GUTTER, GLOBAL_GUTTER, GLOBAL_GUTTER)
        sidebar_layout.setSpacing(12)
        sidebar_layout.addWidget(SectionTitle("MENU"))

        self.mode_buttons: dict[str, SidebarButton] = {}
        for mode in ["OVERLAP", "CROP", "CONVERT", "REPLACE"]:
            key = mode.lower()
            button = SidebarButton(mode)
            sidebar_layout.addWidget(button)
            self.mode_buttons[key] = button
        sidebar_layout.addStretch(1)
        root_layout.addWidget(self.sidebar)

        self.content = QWidget()
        content_layout = QVBoxLayout(self.content)
        content_layout.setContentsMargins(0, 0, 0, 0)
        content_layout.setSpacing(GLOBAL_GUTTER)

        self.action_panel = SurfaceFrame()
        action_layout = QVBoxLayout(self.action_panel)
        action_layout.setContentsMargins(GLOBAL_GUTTER, GLOBAL_GUTTER, GLOBAL_GUTTER, GLOBAL_GUTTER)
        action_layout.setSpacing(GLOBAL_GUTTER)
        action_layout.addWidget(SectionTitle("ACTION"))
        action_row = QHBoxLayout()
        action_row.setContentsMargins(0, 0, 0, 0)
        action_row.setSpacing(GLOBAL_GUTTER)
        self.run_button = ActionButton("RUN", emphasized=True)
        self.pause_button = ActionButton("PAUSE")
        self.stop_button = ActionButton("STOP")
        action_row.addWidget(self.run_button)
        action_row.addWidget(self.pause_button)
        action_row.addWidget(self.stop_button)
        action_row.addStretch(1)
        action_layout.addLayout(action_row)
        content_layout.addWidget(self.action_panel)

        self.stack = QStackedWidget()
        self.stack.addWidget(self._build_overlap_panel())
        self.stack.addWidget(self._build_crop_panel())
        self.stack.addWidget(self._build_convert_panel())
        self.stack.addWidget(self._build_replace_panel())
        content_layout.addWidget(self.stack, 1)

        self.progress_panel = SurfaceFrame()
        progress_layout = QVBoxLayout(self.progress_panel)
        progress_layout.setContentsMargins(GLOBAL_GUTTER, GLOBAL_GUTTER, GLOBAL_GUTTER, GLOBAL_GUTTER)
        progress_layout.setSpacing(GLOBAL_GUTTER)
        progress_layout.addWidget(SectionTitle("PROGRESS"))
        progress_row = QHBoxLayout()
        progress_row.setContentsMargins(0, 0, 0, 0)
        progress_row.setSpacing(GLOBAL_GUTTER)
        self.progress_bar = QProgressBar()
        self.progress_bar.setTextVisible(False)
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)
        self.progress_bar.setFixedHeight(FIELD_HEIGHT)
        self.progress_bar.setProperty("progressBar", True)
        self.progress_label = QLabel("0%")
        self.progress_label.setProperty("progressPercent", True)
        progress_row.addWidget(self.progress_bar, 1)
        progress_row.addWidget(self.progress_label, 0, Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        progress_layout.addLayout(progress_row)
        content_layout.addWidget(self.progress_panel)

        self.log_panel = SurfaceFrame()
        log_layout = QVBoxLayout(self.log_panel)
        log_layout.setContentsMargins(GLOBAL_GUTTER, GLOBAL_GUTTER, GLOBAL_GUTTER, GLOBAL_GUTTER)
        log_layout.setSpacing(GLOBAL_GUTTER)
        log_layout.addWidget(SectionTitle("LOG"))
        self.log_view = QPlainTextEdit()
        self.log_view.setReadOnly(True)
        self.log_view.setProperty("logView", True)
        log_layout.addWidget(self.log_view, 1)
        content_layout.addWidget(self.log_panel, 1)

        root_layout.addWidget(self.content, 1)
        self.setCentralWidget(root)

    def _build_overlap_panel(self) -> QWidget:
        panel = SurfaceFrame()
        layout = QVBoxLayout(panel)
        layout.setContentsMargins(GLOBAL_GUTTER, GLOBAL_GUTTER, GLOBAL_GUTTER, GLOBAL_GUTTER)
        layout.setSpacing(GLOBAL_GUTTER)
        self.overlap_source_row = DirectoryRow("Source")
        self.overlap_multiplier_row = NumberRow("Multiplier", f"{PY_WIDTH_OVERLAP_REL_THRESHOLD:.2f}")
        layout.addWidget(self.overlap_source_row)
        layout.addWidget(self.overlap_multiplier_row)
        layout.addStretch(1)
        return panel

    def _build_crop_panel(self) -> QWidget:
        panel = SurfaceFrame()
        layout = QVBoxLayout(panel)
        layout.setContentsMargins(GLOBAL_GUTTER, GLOBAL_GUTTER, GLOBAL_GUTTER, GLOBAL_GUTTER)
        layout.setSpacing(GLOBAL_GUTTER)
        self.crop_source_row = DirectoryRow("Source")
        self.crop_ratio_row = NumberRow("Ratio", f"{DEFAULT_CROP_RATIO:.3f}")
        layout.addWidget(self.crop_source_row)
        layout.addWidget(self.crop_ratio_row)
        layout.addStretch(1)
        return panel

    def _build_convert_panel(self) -> QWidget:
        wrapper = QWidget()
        layout = QVBoxLayout(wrapper)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(GLOBAL_GUTTER)
        self.pdf_to_jpeg_panel = ConvertPanel("PDF -> JPEG", "pdf-to-jpeg")
        self.jpeg_to_pdf_panel = ConvertPanel("JPEG -> PDF", "jpeg-to-pdf")
        layout.addWidget(self.pdf_to_jpeg_panel)
        layout.addWidget(self.jpeg_to_pdf_panel)
        layout.addStretch(1)
        return wrapper

    def _build_replace_panel(self) -> QWidget:
        panel = SurfaceFrame()
        layout = QVBoxLayout(panel)
        layout.setContentsMargins(GLOBAL_GUTTER, GLOBAL_GUTTER, GLOBAL_GUTTER, GLOBAL_GUTTER)
        layout.setSpacing(GLOBAL_GUTTER)
        self.replace_cropped_row = DirectoryRow("Cropped")
        self.replace_target_row = DirectoryRow("Replacement")
        layout.addWidget(self.replace_cropped_row)
        layout.addWidget(self.replace_target_row)
        layout.addStretch(1)
        return panel

    def _wire_events(self) -> None:
        for mode, button in self.mode_buttons.items():
            button.clicked.connect(lambda _checked=False, value=mode: self._set_mode(value))

        self.overlap_source_row.button.clicked.connect(lambda: self._pick_directory(self.overlap_source_row))
        self.crop_source_row.button.clicked.connect(lambda: self._pick_directory(self.crop_source_row))
        self.pdf_to_jpeg_panel.directory_row.button.clicked.connect(lambda: self._pick_directory(self.pdf_to_jpeg_panel.directory_row))
        self.jpeg_to_pdf_panel.directory_row.button.clicked.connect(lambda: self._pick_directory(self.jpeg_to_pdf_panel.directory_row))
        self.replace_cropped_row.button.clicked.connect(lambda: self._pick_directory(self.replace_cropped_row))
        self.replace_target_row.button.clicked.connect(lambda: self._pick_directory(self.replace_target_row))

        self.pdf_to_jpeg_panel.button.clicked.connect(lambda: self._set_convert_action("pdf-to-jpeg"))
        self.jpeg_to_pdf_panel.button.clicked.connect(lambda: self._set_convert_action("jpeg-to-pdf"))

        self.run_button.clicked.connect(self._start_run)
        self.pause_button.clicked.connect(self._toggle_pause)
        self.stop_button.clicked.connect(self._stop_run)

    def _apply_styles(self) -> None:
        self.setStyleSheet(
            """
            QMainWindow, QWidget {
                background: #f6f6f4;
                color: #111111;
            }
            QFrame#sidebar {
                background: rgba(255, 255, 255, 235);
                border: none;
                border-radius: 18px;
            }
            QFrame#surface {
                background: rgba(255, 255, 255, 244);
                border: none;
                border-radius: 18px;
            }
            QFrame[selectableSurface="true"][activeSurface="true"] {
                background: rgba(248, 248, 246, 255);
                border: 1px solid #111111;
            }
            QFrame[selectableSurface="true"][activeSurface="false"] {
                background: rgba(255, 255, 255, 244);
                border: 1px solid rgba(17, 17, 17, 0);
            }
            QLabel[sectionTitle="true"] {
                font-size: 12px;
                font-weight: 600;
                letter-spacing: 0.12em;
                text-transform: uppercase;
                color: #4b4b49;
            }
            QLabel[fieldLabel="true"] {
                font-size: 12px;
                font-weight: 600;
                letter-spacing: 0.12em;
                color: #4b4b49;
            }
            QLabel[progressPercent="true"] {
                font-size: 12px;
                font-weight: 600;
                letter-spacing: 0.08em;
                color: #4b4b49;
                min-width: 56px;
            }
            QPushButton[sidebarButton="true"] {
                background: transparent;
                border: none;
                border-radius: 10px;
                padding: 0 14px;
                text-align: left;
                font-size: 13px;
                font-weight: 600;
                letter-spacing: 0.14em;
                color: #6a6a67;
            }
            QPushButton[sidebarButton="true"]:checked {
                background: #d7d7d2;
                color: #111111;
            }
            QPushButton[actionButton="true"] {
                background: #f0f0ed;
                border: none;
                border-radius: 10px;
                padding: 0 14px;
                font-size: 13px;
                font-weight: 600;
                letter-spacing: 0.14em;
                color: #111111;
            }
            QPushButton[actionButton="true"][emphasized="true"] {
                background: #111111;
                color: #ffffff;
            }
            QPushButton[browseButton="true"], QPushButton[convertSelector="true"] {
                background: #efefec;
                border: none;
                border-radius: 10px;
                padding: 0 14px;
                font-size: 12px;
                font-weight: 600;
                letter-spacing: 0.12em;
                color: #111111;
            }
            QLineEdit[formField="true"] {
                background: #efefec;
                border: none;
                border-radius: 10px;
                padding: 0 12px;
                font-size: 14px;
                font-weight: 500;
                color: #111111;
            }
            QProgressBar[progressBar="true"] {
                background: #ebebe7;
                border: none;
                border-radius: 10px;
            }
            QProgressBar[progressBar="true"]::chunk {
                background: #8c8c87;
                border-radius: 10px;
            }
            QPlainTextEdit[logView="true"] {
                background: transparent;
                border: none;
                font-size: 13px;
                font-weight: 500;
                color: #30302e;
                selection-background-color: #d7d7d2;
            }
            """
        )

    def _pick_directory(self, row: DirectoryRow) -> None:
        start_dir = row.text() or str(Path.home())
        selected = QFileDialog.getExistingDirectory(self, APP_NAME, start_dir)
        if selected:
            row.set_text(selected)

    def _set_mode(self, mode: str) -> None:
        self.current_mode = mode
        self._refresh_mode()

    def _set_convert_action(self, action: str) -> None:
        self.current_convert_action = action
        self._refresh_convert_action()

    def _refresh_mode(self) -> None:
        index_map = {
            "overlap": 0,
            "crop": 1,
            "convert": 2,
            "replace": 3,
        }
        for mode, button in self.mode_buttons.items():
            button.setChecked(mode == self.current_mode)
        self.stack.setCurrentIndex(index_map[self.current_mode])
        self.setWindowTitle(APP_NAME)

    def _refresh_convert_action(self) -> None:
        is_pdf = self.current_convert_action == "pdf-to-jpeg"
        self.pdf_to_jpeg_panel.set_active(is_pdf)
        self.jpeg_to_pdf_panel.set_active(not is_pdf)

    def _active_operation_name(self) -> str:
        if self.current_mode == "convert":
            return self.current_convert_action
        return self.current_mode

    def _clear_run_state(self) -> None:
        self.log_view.clear()
        self.progress_done = 0
        self.progress_total = 1
        self.progress_bar.setValue(0)
        self.progress_label.setText("0%")
        self.pause_button.setText("PAUSE")

    def _append_log(self, message: str) -> None:
        if not message:
            return
        self.log_view.appendPlainText(message)
        scrollbar = self.log_view.verticalScrollBar()
        scrollbar.setValue(scrollbar.maximum())

    def _set_progress(self, done: int, total: int) -> None:
        self.progress_done = done
        self.progress_total = max(total, 1)
        value = int(round((self.progress_done / self.progress_total) * 100))
        self.progress_bar.setValue(max(0, min(value, 100)))
        self.progress_label.setText(f"{value}%")

    def _validate_run(self) -> Optional[str]:
        if self.current_mode == "overlap":
            if not Path(self.overlap_source_row.text()).is_dir():
                return "Source directory does not exist."
            try:
                if float(self.overlap_multiplier_row.text()) <= 0:
                    return "Multiplier must be greater than zero."
            except Exception:
                return "Multiplier is invalid."
        elif self.current_mode == "crop":
            if not Path(self.crop_source_row.text()).is_dir():
                return "Crop directory does not exist."
            try:
                if float(self.crop_ratio_row.text()) <= 0:
                    return "Ratio must be greater than zero."
            except Exception:
                return "Ratio is invalid."
        elif self.current_mode == "convert":
            selected_row = self.pdf_to_jpeg_panel.directory_row if self.current_convert_action == "pdf-to-jpeg" else self.jpeg_to_pdf_panel.directory_row
            if not Path(selected_row.text()).is_dir():
                return "Convert directory does not exist."
        else:
            if not Path(self.replace_cropped_row.text()).is_dir():
                return "Cropped directory does not exist."
            if not Path(self.replace_target_row.text()).is_dir():
                return "Replacement directory does not exist."
        return None

    def _start_run(self) -> None:
        if self.worker_thread and self.worker_thread.is_alive():
            QMessageBox.warning(self, APP_NAME, "A run is already active.")
            return

        error = self._validate_run()
        if error:
            QMessageBox.warning(self, APP_NAME, error)
            return

        request = self._build_run_request()
        self._clear_run_state()
        self.cancel_event = threading.Event()
        self.pause_event = threading.Event()
        self.worker_thread = threading.Thread(
            target=self._run_operation,
            args=(request,),
            daemon=True,
        )
        self.worker_thread.start()

    def _toggle_pause(self) -> None:
        if not self.worker_thread or not self.worker_thread.is_alive():
            return
        if self.pause_event.is_set():
            self.pause_event.clear()
            self.pause_button.setText("PAUSE")
            self._append_log("Pipeline resumed.")
        else:
            self.pause_event.set()
            self.pause_button.setText("RESUME")
            self._append_log("Pipeline paused.")

    def _stop_run(self) -> None:
        if not self.worker_thread or not self.worker_thread.is_alive():
            return
        self.cancel_event.set()
        self.pause_event.clear()
        self.pause_button.setText("PAUSE")
        self._append_log("Stop requested.")

    def _enqueue(self, event_type: str, **payload: Any) -> None:
        self.event_queue.put((event_type, payload))

    def _build_run_request(self) -> dict[str, Any]:
        operation = self._active_operation_name()
        request: dict[str, Any] = {"operation": operation}
        if operation == "overlap":
            request["source_dir"] = self.overlap_source_row.text()
            request["overlap_multiplier"] = float(self.overlap_multiplier_row.text())
        elif operation == "crop":
            request["source_dir"] = self.crop_source_row.text()
            request["crop_ratio"] = float(self.crop_ratio_row.text())
        elif operation == "pdf-to-jpeg":
            request["source_dir"] = self.pdf_to_jpeg_panel.directory_row.text()
        elif operation == "jpeg-to-pdf":
            request["source_dir"] = self.jpeg_to_pdf_panel.directory_row.text()
        else:
            request["cropped_dir"] = self.replace_cropped_row.text()
            request["replacement_dir"] = self.replace_target_row.text()
        return request

    def _run_operation(self, request: dict[str, Any]) -> None:
        operation = str(request["operation"])
        controller = PipelineController(
            cancel_event=self.cancel_event,
            pause_event=self.pause_event,
        )
        hooks = PipelineHooks(
            log=lambda message: self._enqueue("log", message=message),
            status=lambda message: self._enqueue("status", message=message),
            progress=lambda done, total: self._enqueue("progress", done=done, total=total),
            overlap_estimate=lambda pdf_path, info: self._enqueue(
                "estimate",
                file_name=pdf_path.name,
                text=f"{float(info.get('baseline_body_width') or 0.0):.0f}",
            ),
            replace_cropped_dir=lambda output_dir: self._enqueue(
                "suggested-cropped-dir",
                path=str(output_dir),
            ),
        )

        try:
            if operation == "overlap":
                source_dir = Path(str(request["source_dir"])).expanduser()
                batch_root = batch_root_for_path(source_dir)
                storage = Storage()
                memory = ensure_memory_schema(storage.load_memory())
                result = run_overlap_pipeline(
                    OverlapRunConfig(
                        source_dir=source_dir,
                        batch_root=batch_root,
                        estimate_csv_path=batch_root / "estimated_widths.csv",
                        problem_csv_path=batch_root / "problem_pages.csv",
                        run_log_path=batch_root / f"overlap_run_{_BACKEND.now_file_ts()}.txt",
                        parameter_override={"overlap_multiplier": float(request["overlap_multiplier"])},
                        export_csv=True,
                        export_overlap_pages=True,
                        export_extracted_original=True,
                    ),
                    hooks=hooks,
                    controller=controller,
                    storage=storage,
                    memory=memory,
                )
            elif operation == "crop":
                source_dir = Path(str(request["source_dir"])).expanduser()
                result = run_crop_pipeline(
                    CropRunConfig(
                        source_dir=source_dir,
                        cropped_dir=source_dir / "cropped",
                        uncropped_dir=source_dir / "uncropped",
                        crop_ratio=float(request["crop_ratio"]),
                        run_log_path=source_dir / "cropped" / "crop_run_ui.txt",
                    ),
                    hooks=hooks,
                    controller=controller,
                )
            elif operation == "pdf-to-jpeg":
                source_dir = Path(str(request["source_dir"])).expanduser()
                result = run_pdf_to_jpeg_pipeline(
                    PdfToJpegRunConfig(
                        source_dir=source_dir,
                        output_dir=source_dir / "JPEG",
                        run_log_path=source_dir / "JPEG" / "pdf-to-jpeg-run.txt",
                    ),
                    hooks=hooks,
                    controller=controller,
                )
            elif operation == "jpeg-to-pdf":
                source_dir = Path(str(request["source_dir"])).expanduser()
                result = run_jpeg_to_pdf_pipeline(
                    JpegToPdfRunConfig(
                        source_dir=source_dir,
                        output_dir=source_dir / "PDF",
                        run_log_path=source_dir / "PDF" / "jpeg-to-pdf-run.txt",
                    ),
                    hooks=hooks,
                    controller=controller,
                )
            elif operation == "replace":
                replacement_dir = Path(str(request["replacement_dir"])).expanduser()
                result = run_replace_pipeline(
                    ReplaceRunConfig(
                        cropped_dir=Path(str(request["cropped_dir"])).expanduser(),
                        replacement_dir=replacement_dir,
                        run_log_path=replacement_dir / "replace_run_ui.txt",
                    ),
                    hooks=hooks,
                    controller=controller,
                )
            else:
                raise RuntimeError(f"Unsupported operation: {operation}")

            self._enqueue("result", operation=operation, result=result)
        except Exception as exc:
            self._enqueue(
                "error",
                message=str(exc),
                details=traceback.format_exc(),
            )
        finally:
            self._enqueue("finished")

    def _drain_events(self) -> None:
        while True:
            try:
                event_type, payload = self.event_queue.get_nowait()
            except queue.Empty:
                break

            if event_type == "log":
                self._append_log(str(payload.get("message", "")))
            elif event_type == "progress":
                self._set_progress(int(payload.get("done", 0)), int(payload.get("total", 1)))
            elif event_type == "suggested-cropped-dir":
                path = str(payload.get("path", ""))
                if path:
                    self.replace_cropped_row.set_text(path)
            elif event_type == "error":
                self._append_log(str(payload.get("message", "")))
                details = str(payload.get("details", "")).strip()
                if details:
                    self._append_log(details)
            elif event_type == "result":
                result = payload.get("result") or {}
                operation = str(payload.get("operation", ""))
                self._append_summary(operation, result)
            elif event_type == "finished":
                self.pause_button.setText("PAUSE")
                self.worker_thread = None

    def _append_summary(self, operation: str, result: dict[str, Any]) -> None:
        if not result:
            return
        if operation == "overlap":
            self._append_log(
                f"OVERLAP {int(result.get('overlap_count', 0))} "
                f"CLEAN {int(result.get('clean_count', 0))} "
                f"UNCERTAIN {int(result.get('uncertain_count', 0))}"
            )
        elif operation == "crop":
            self._append_log(
                f"CROPPED {int(result.get('cropped_count', 0))} "
                f"UNCROPPED {int(result.get('uncropped_count', 0))} "
                f"ERRORS {int(result.get('error_count', 0))}"
            )
        elif operation in {"pdf-to-jpeg", "jpeg-to-pdf"}:
            noun = "JPEG" if operation == "pdf-to-jpeg" else "PDF"
            self._append_log(f"{noun} {int(result.get('created_count', 0))} ERRORS {int(result.get('error_count', 0))}")
        elif operation == "replace":
            self._append_log(
                f"REPLACED {int(result.get('replaced_count', 0))} "
                f"ERRORS {int(result.get('error_count', 0))}"
            )

    def closeEvent(self, event) -> None:  # type: ignore[override]
        if self.worker_thread and self.worker_thread.is_alive():
            self.cancel_event.set()
            self.pause_event.clear()
            self.worker_thread.join(timeout=2.0)
        super().closeEvent(event)


def main() -> None:
    global APP_FONT_FAMILY
    os.environ.setdefault("QT_ENABLE_HIGHDPI_SCALING", "1")
    os.environ.setdefault("QT_AUTO_SCREEN_SCALE_FACTOR", "1")
    app = QApplication(sys.argv)
    APP_FONT_FAMILY = _load_app_font()
    app.setStyle("Fusion")
    app.setApplicationDisplayName(APP_NAME)
    app.setFont(_make_font(13))
    window = FrontendWindow()
    window.show()
    raise SystemExit(app.exec())


if __name__ == "__main__":
    main()
