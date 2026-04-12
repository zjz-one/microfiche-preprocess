# -*- mode: python ; coding: utf-8 -*-

from pathlib import Path
import ast


def _hidden_imports_from(source_path: Path) -> set[str]:
    tree = ast.parse(source_path.read_text(encoding="utf-8"))
    modules: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for imported in node.names:
                modules.add(imported.name)
        elif isinstance(node, ast.ImportFrom) and node.module:
            modules.add(node.module)
    return modules


spec_root = Path.cwd()
hiddenimports = (
    _hidden_imports_from(spec_root / "microfiche-preprocess.py")
    | _hidden_imports_from(spec_root / "microfiche-preprocess-cli.py")
    | {"PIL.Image", "PIL.ImageOps"}
) - {"__future__"}

a = Analysis(
    ["microfiche-preprocess-gui.py"],
    pathex=[],
    binaries=[],
    datas=[
        ("microfiche-preprocess.py", "."),
        ("microfiche-preprocess-cli.py", "."),
    ],
    hiddenimports=sorted(hiddenimports),
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    exclude_binaries=False,
    name="microfiche-preprocess",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=False,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    icon="microfiche-preprocess.ico",
)
