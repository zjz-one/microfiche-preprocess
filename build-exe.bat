@echo off
setlocal

python -m pip install --upgrade pip
if errorlevel 1 exit /b 1

python -m pip install -r requirements.txt pyinstaller
if errorlevel 1 exit /b 1

python -m PyInstaller --noconfirm --clean microfiche-preprocess.spec
if errorlevel 1 exit /b 1

echo Build complete: dist\microfiche-preprocess.exe
