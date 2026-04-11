# Ops Monitor - Step 13 Phase 1 Wrap-Up

This is **Step 13** of the desktop-first Ops Monitor application.

It is the **Phase 1 wrap-up build** and includes the Phase 1 features developed so far:
- desktop login and role-aware UI
- dashboard with grouped check tiles
- Site 1 / Site 2 switching
- parser engine + monitoring loop using local test data
- alert lifecycle + acknowledgment
- check details, history, and graph views
- in-app configuration for checks and groups
- audit and settings tabs
- theme switching, export, and backup actions
- persistent tile hover/selection during refresh cycles

## Requirements

- Python 3.11+
- PySide6

Install dependencies:

```bash
pip install -r requirements.txt
```

## Run

Extract to a **new folder** so the database can initialize cleanly.

```bash
python main.py
```

## Default login

- Username: `admin`
- Password: `admin123`

## Packaging

A Windows helper file is included:

```text
build_exe.bat
```

This uses **PyInstaller** to prepare an `.exe` build.

See also:
- `DEPLOYMENT.md`
- `PACKAGE_EXE.md`

## Recommended folder layout

```text
OpsMonitor/
    main.py
    build_exe.bat
    config/
    src/
    Logs/
    Backups/
    TestData/
        Site1/
        Site2/
```

## Included sample test files

`TestData/Site1`
- `checkQ.log` -> unhealthy
- `hunglocks.log` -> healthy
- `mpReg1.log` -> healthy
- `mpReg3.log` -> intentionally missing to simulate stale
- `versantcpu.log` -> healthy

`TestData/Site2`
- `checkQ.log` -> healthy
- `hunglocks.log` -> unhealthy
- `mpReg1.log` -> healthy
- `mpReg3.log` -> healthy
- `versantcpu.log` -> unhealthy

Use **Switch Site** in the header to compare Site 1 and Site 2 test data.

## Phase 1 validation checklist

- [x] Login works
- [x] Dashboard refresh works
- [x] Site switching works
- [x] Parser test works
- [x] Alerts and acknowledgments work
- [x] History / graphs work
- [x] Config export works
- [x] Database backup works
- [x] Issue-only filter works
- [x] Compact mode works
- [x] Packaging helper included

## Next recommended direction

After this Step 13 wrap-up, the best next move is a **Phase 1.5 refinement pass** for UI tweaks and quality-of-life improvements before connecting live data in Phase 2.


## Step 47.1 patch

This patch restores the Step 46 runtime mode and live data root settings while keeping Step 47 notification rules and maintenance windows.


## Build 48.2
- Refined dual-mode layout with a calmer monitoring view and a timeline-first incident command center.
- Rebalanced incident workspace columns to keep the timeline feed centered and dominant.
- Added compact incident status badges and reduced button stacking to cut clutter.
