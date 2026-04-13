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
    | _hidden_imports_from(spec_root / "pdf-playboard-gui.py")
    | {"PIL.Image", "PIL.ImageOps"}
) - {"__future__"}


a = Analysis(
    ["microfiche-preprocess-gui.py"],
    pathex=[],
    binaries=[],
    datas=[
        ("microfiche-preprocess.py", "."),
        ("microfiche-preprocess-cli.py", "."),
        ("pdf-playboard-gui.py", "."),
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
    name="microfiche-preprocess",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon="microfiche-preprocess.ico",
)
