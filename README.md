# microfiche-preprocess Windows build

This folder contains the current Windows packaging source for `microfiche-preprocess`.

Upload the contents of this folder to a GitHub repository root, not the `win/` folder itself.

Required root files after upload:

- `.github/workflows/build-windows-exe.yml`
- `build-exe.bat`
- `microfiche-preprocess.py`
- `microfiche-preprocess-cli.py`
- `microfiche-preprocess-gui.py`
- `microfiche-preprocess.spec`
- `microfiche-preprocess.ico`
- `requirements.txt`

Then run the `Build Windows EXE` workflow from GitHub Actions.

The artifact will contain:

- `microfiche-preprocess.exe`
- the bundled runtime files from `dist/microfiche-preprocess/`
