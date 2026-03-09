# Microfiche Problem Detector

Windows build package for the current third-party app.

## Required files

- `problem_detector_exe.py`
  Windows EXE entrypoint
- `problem_detector_llm.py`
  Main GUI and LLM logic
- `problem_detector_py.py`
  Standalone pure-Python detector script
- `requirements.txt`
- `build_exe.bat`
- `.github/workflows/build-windows-exe.yml`

## Upload to GitHub

Upload the contents of this folder as the repo root.

Expected repo structure:

```text
repo-root/
  problem_detector_exe.py
  problem_detector_llm.py
  problem_detector_py.py
  requirements.txt
  build_exe.bat
  .github/
    workflows/
      build-windows-exe.yml
```

## Build on GitHub

1. Push the repo to GitHub.
2. Open `Actions`.
3. Open `Build Windows EXE`.
4. Click `Run workflow`.
5. Download artifact `MicroficheProblemDetector-windows`.

Expected exe path inside the build output:

```text
dist/MicroficheProblemDetector.exe
```

## Build on Windows locally

Run:

```bat
build_exe.bat
```

If the build succeeds, the exe will be here:

```text
dist\MicroficheProblemDetector.exe
```

If it fails, inspect:

```text
build.log
```

## App behavior

Modes:
- `LLM`
- `PY`

Outputs:
- `CSV`
- `Overlap`
- `Blurry`
- `Extracted Original`
- `U_*.pdf` uncertain export is automatic

Rules:
- CSV includes overlap, blurry, and uncertain pages.
- `Extracted Original` removes overlap and blurry pages.
- `Extracted Original` keeps uncertain pages.
- `LLM` mode uses configured API models.
- `PY` mode is local-only and sends no API requests.
