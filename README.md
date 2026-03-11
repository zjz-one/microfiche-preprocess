# Microfiche Problem Detector (Windows Build Files)

Files in this folder are the repository contents for building `MicroficheProblemDetector.exe` on GitHub Actions or on Windows.

## Included
- `microfiche_problem_detector.py`
- `problem_detector_exe.py`
- `requirements.txt`
- `build_exe.bat`
- `.github/workflows/build-windows-exe.yml`

## GitHub Actions
1. Upload the contents of this folder as the root of a GitHub repository.
2. Go to `Actions`.
3. Run `Build Windows EXE`.
4. Download artifact `MicroficheProblemDetector-windows`.

## Local Windows build
Run:

```bat
build_exe.bat
```

Output:
- `dist\\MicroficheProblemDetector.exe`
