#!/usr/bin/env python3
from __future__ import annotations

import importlib.util
from pathlib import Path


def _load_main():
    script_path = Path(__file__).with_name("microfiche-preprocess.py")
    spec = importlib.util.spec_from_file_location("microfiche_preprocess_app", script_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Failed to load {script_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.main


if __name__ == "__main__":
    _load_main()()
