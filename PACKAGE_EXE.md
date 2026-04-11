# Packaging Ops Monitor into a Windows EXE

## Prerequisites
- Windows
- Python 3.11+
- `pip`

## Build
From the project root run:

```bat
build_exe.bat
```

This will:
1. install dependencies
2. install PyInstaller
3. build the app into `dist/OpsMonitor/`

## Expected output
```text
dist/
    OpsMonitor/
        OpsMonitor.exe
        _internal/
```

## Recommended packaging test
After building, verify:
- app starts
- login works
- dashboard loads
- site switching works
- configuration tab opens
- backup/export actions work
