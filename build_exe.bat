@echo off
setlocal
cd /d "%~dp0"

REM Build Ops Monitor into a single-folder Windows executable using PyInstaller.
REM Run from the project root after installing requirements.

py -m pip install -r requirements.txt
py -m pip install pyinstaller

py -m PyInstaller --noconfirm --clean --paths "%cd%" --hidden-import src.ui.main_window --hidden-import src.ui.login_window --hidden-import src.ui.config_dialogs --hidden-import src.ui.history_chart OpsMonitor.spec

echo.
echo Build complete. See the dist\OpsMonitor folder.
pause
