# -*- mode: python ; coding: utf-8 -*-


a = Analysis(
    ["microfiche-preprocess-gui.py"],
    pathex=[],
    binaries=[],
    datas=[
        ("microfiche-preprocess.py", "."),
        ("microfiche-preprocess-cli.py", "."),
    ],
    hiddenimports=[],
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
