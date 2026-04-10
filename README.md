# Microfiche Preprocess Windows Pack

Upload the contents of this `win/` folder as the root of a GitHub repository.

Included:
- `microfiche-preprocess.py`
- `microfiche-preprocess-exe.py`
- `microfiche-preprocess-cli.py`
- `requirements.txt`
- `build-exe.bat`
- `.github/workflows/build-windows-exe.yml`

## GitHub Actions

1. Create a new GitHub repository.
2. Upload every file and folder inside `win/` to the repo root.
3. Open `Actions`.
4. Run `Build Windows EXE`.
5. Download artifact `Microfiche-Preprocess-windows`.

## Local Windows Build

Run:

```bat
build-exe.bat
```

Output:

- `dist/Microfiche-Preprocess.exe`

## Notes

- The EXE is built from `microfiche-preprocess.py`.
- The current Windows UI is the Python desktop UI, not the native macOS SwiftUI shell.
