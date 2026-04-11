# -*- mode: python ; coding: utf-8 -*-
from pathlib import Path
from PyInstaller.utils.hooks import collect_submodules

project_root = Path(__file__).resolve().parent
hiddenimports = [
    "src",
    "src.ui",
    "src.ui.main_window",
    "src.ui.login_window",
    "src.ui.config_dialogs",
    "src.ui.history_chart",
    "src.application",
    "src.application.bootstrap",
    "src.application.config_loader",
    "src.application.logging_setup",
    "src.data",
    "src.data.database",
    "src.data.auth_repository",
    "src.data.monitoring_repository",
    "src.data.dashboard_repository",
    "src.data.settings_repository",
    "src.data.config_repository",
    "src.data.user_repository",
    "src.data.seed",
    "src.engine",
    "src.engine.monitoring_engine",
    "src.engine.parser_engine",
    "src.shared",
    "src.shared.constants",
    "src.shared.models",
    "src.theming",
    "src.theming.theme_service",
]
hiddenimports += collect_submodules("src")

a = Analysis(
    [str(project_root / "main.py")],
    pathex=[str(project_root)],
    binaries=[],
    datas=[(str(project_root / "config"), "config"), (str(project_root / "TestData"), "TestData")],
    hiddenimports=hiddenimports,
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
    [],
    exclude_binaries=True,
    name="OpsMonitor",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name="OpsMonitor",
)
