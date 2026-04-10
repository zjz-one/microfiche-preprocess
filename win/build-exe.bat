@echo off
setlocal EnableExtensions EnableDelayedExpansion

cd /d %~dp0
set LOG=%cd%\build.log
echo ==== Build start %date% %time% ==== > "%LOG%"

set PY=
where py >nul 2>nul
if %errorlevel%==0 (
  set PY=py -3
) else (
  where python >nul 2>nul
  if %errorlevel%==0 (
    set PY=python
  ) else (
    echo [ERROR] Python not found. Install Python 3.11+ first.
    echo Python not found >> "%LOG%"
    goto :fail
  )
)

echo [info] Using launcher: %PY%
echo Using launcher: %PY% >> "%LOG%"

echo [1/6] Create venv...
%PY% -m venv .venv-build >> "%LOG%" 2>&1
if errorlevel 1 goto :fail

call .venv-build\Scripts\activate.bat
if errorlevel 1 (
  echo activate failed >> "%LOG%"
  goto :fail
)

echo [2/6] Python version...
python --version >> "%LOG%" 2>&1
if errorlevel 1 goto :fail

echo [3/6] Upgrade pip/setuptools/wheel...
python -m pip install --upgrade pip setuptools wheel >> "%LOG%" 2>&1
if errorlevel 1 goto :fail

echo [4/6] Install deps...
python -m pip install -r requirements.txt pyinstaller >> "%LOG%" 2>&1
if errorlevel 1 goto :fail

echo [5/6] Build exe...
python -m PyInstaller --noconfirm --clean --onefile --windowed --name Microfiche-Preprocess --icon microfiche-preprocess.ico --collect-all PySide6 --collect-all shiboken6 --collect-all fitz --collect-all PIL --hidden-import tkinter --hidden-import tkinter.ttk --hidden-import tkinter.filedialog --hidden-import tkinter.messagebox --hidden-import tkinter.font --hidden-import tkinter.scrolledtext --add-data "microfiche-preprocess.py;." --add-data "microfiche-preprocess-cli.py;." --add-data "fonts;fonts" microfiche-preprocess-ui.py >> "%LOG%" 2>&1
if errorlevel 1 goto :fail

echo [6/6] Verify output...
if not exist "%cd%\dist\Microfiche-Preprocess.exe" (
  echo dist exe missing >> "%LOG%"
  goto :fail
)

echo Build succeeded.
echo EXE: %cd%\dist\Microfiche-Preprocess.exe
echo Build succeeded >> "%LOG%"
echo ==== Build end %date% %time% ==== >> "%LOG%"
exit /b 0

:fail
echo Build failed. See log: %LOG%
echo ==== Build failed %date% %time% ==== >> "%LOG%"
exit /b 1
