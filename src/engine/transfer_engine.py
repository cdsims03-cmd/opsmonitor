from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
import fnmatch
import json
import logging
import shutil
import subprocess
import threading
import time
from typing import Any

logger = logging.getLogger(__name__)


DEFAULT_MAPPINGS: list[dict[str, Any]] = [
    {"enabled": True, "name": "Version 103", "remote_pattern": "check_ver-103*ftp.log", "stable_local_name": "ver103.log", "destination_subfolder": "", "site_scope": "Both", "use_newest": True, "keep_raw_copy": False},
    {"enabled": True, "name": "Version 104", "remote_pattern": "check_ver-104*ftp.log", "stable_local_name": "ver104.log", "destination_subfolder": "", "site_scope": "Both", "use_newest": True, "keep_raw_copy": False},
    {"enabled": True, "name": "ARE", "remote_pattern": "check_ARE*.log", "stable_local_name": "ARE.log", "destination_subfolder": "", "site_scope": "Both", "use_newest": True, "keep_raw_copy": False},
    {"enabled": True, "name": "Proc Mem", "remote_pattern": "checkProcMem*.log", "stable_local_name": "CheckProcMem.log", "destination_subfolder": "", "site_scope": "Both", "use_newest": True, "keep_raw_copy": False},
    {"enabled": True, "name": "CheckQ", "remote_pattern": "checkQ*.log", "stable_local_name": "checkQ.log", "destination_subfolder": "", "site_scope": "Both", "use_newest": True, "keep_raw_copy": False},
    {"enabled": True, "name": "Failover", "remote_pattern": "check_failover*.log", "stable_local_name": "failover.log", "destination_subfolder": "", "site_scope": "Both", "use_newest": True, "keep_raw_copy": False},
    {"enabled": True, "name": "Health Core", "remote_pattern": "check_healthUTCS_core*.log", "stable_local_name": "healthUTCS_core.log", "destination_subfolder": "", "site_scope": "Both", "use_newest": True, "keep_raw_copy": False},
    {"enabled": True, "name": "Hung Locks", "remote_pattern": "check_hungLocks_*_ftp.log", "stable_local_name": "hunglocks.log", "destination_subfolder": "", "site_scope": "Both", "use_newest": True, "keep_raw_copy": False},
    {"enabled": True, "name": "ULOG Server", "remote_pattern": "check_ulog_*_ftp.log", "stable_local_name": "ULOGServer.log", "destination_subfolder": "", "site_scope": "Both", "use_newest": True, "keep_raw_copy": False},
    {"enabled": True, "name": "Vref Check", "remote_pattern": "check_vrefcheck_*_ftp.log", "stable_local_name": "vrefcheck.log", "destination_subfolder": "", "site_scope": "Both", "use_newest": True, "keep_raw_copy": False},
    {"enabled": True, "name": "WLS GC", "remote_pattern": "wlsGC_*_ftp.log", "stable_local_name": "wlsGC.log", "destination_subfolder": "", "site_scope": "Both", "use_newest": True, "keep_raw_copy": False},
    {"enabled": True, "name": "WLS Health", "remote_pattern": "wlshealth_*_ftp.log", "stable_local_name": "wlsHealth.log", "destination_subfolder": "", "site_scope": "Both", "use_newest": True, "keep_raw_copy": False},
    {"enabled": True, "name": "Auto Router", "remote_pattern": "check_Auto_Router*.log", "stable_local_name": "autoRouter.log", "destination_subfolder": "", "site_scope": "Both", "use_newest": True, "keep_raw_copy": False},
    {"enabled": True, "name": "CCOP", "remote_pattern": "check_ccop*.log", "stable_local_name": "ccop.log", "destination_subfolder": "", "site_scope": "Both", "use_newest": True, "keep_raw_copy": False},
    {"enabled": True, "name": "MP Reg1", "remote_pattern": "Mp_Reg1*ftp.log", "stable_local_name": "mpReg1.log", "destination_subfolder": "", "site_scope": "Both", "use_newest": True, "keep_raw_copy": False},
    {"enabled": True, "name": "MP Reg7", "remote_pattern": "Mp_Reg7*ftp.log", "stable_local_name": "mpReg7.log", "destination_subfolder": "", "site_scope": "Both", "use_newest": True, "keep_raw_copy": False},
    {"enabled": True, "name": "MP Reg3", "remote_pattern": "Mp_Reg3*ftp.log", "stable_local_name": "mpReg3.log", "destination_subfolder": "", "site_scope": "Both", "use_newest": True, "keep_raw_copy": False},
    {"enabled": True, "name": "MP Reg10", "remote_pattern": "Mp_Reg10*ftp.log", "stable_local_name": "mpReg10.log", "destination_subfolder": "", "site_scope": "Both", "use_newest": True, "keep_raw_copy": False},
    {"enabled": True, "name": "TM Client", "remote_pattern": "tmClient*.log", "stable_local_name": "client.log", "destination_subfolder": "", "site_scope": "Both", "use_newest": True, "keep_raw_copy": False},
    {"enabled": True, "name": "AR Reg1", "remote_pattern": "AR*1*ftp", "stable_local_name": "arReg1.log", "destination_subfolder": "", "site_scope": "Both", "use_newest": True, "keep_raw_copy": False},
    {"enabled": True, "name": "AR Reg7", "remote_pattern": "AR*7*ftp", "stable_local_name": "arReg7.log", "destination_subfolder": "", "site_scope": "Both", "use_newest": True, "keep_raw_copy": False},
    {"enabled": True, "name": "AR Reg3", "remote_pattern": "AR*3*ftp", "stable_local_name": "arReg3.log", "destination_subfolder": "", "site_scope": "Both", "use_newest": True, "keep_raw_copy": False},
    {"enabled": True, "name": "AR Reg10", "remote_pattern": "AR*10*ftp", "stable_local_name": "arReg10.log", "destination_subfolder": "", "site_scope": "Both", "use_newest": True, "keep_raw_copy": False},
    {"enabled": True, "name": "Slowness", "remote_pattern": "check_auto_slownessDataTable*ftp.log", "stable_local_name": "slowness.log", "destination_subfolder": "", "site_scope": "Both", "use_newest": True, "keep_raw_copy": False},
    {"enabled": True, "name": "Trip Plan", "remote_pattern": "TripCount_ftp.log", "stable_local_name": "tripPlan.log", "destination_subfolder": "", "site_scope": "Both", "use_newest": True, "keep_raw_copy": False},
    {"enabled": True, "name": "PTRS", "remote_pattern": "check_healthRDB_ptr*.log", "stable_local_name": "ptrs.log", "destination_subfolder": "", "site_scope": "Both", "use_newest": True, "keep_raw_copy": False},
    {"enabled": True, "name": "MPlan Reg1", "remote_pattern": "MPlan_Reg1*ftp", "stable_local_name": "mPlanReg1.log", "destination_subfolder": "", "site_scope": "Both", "use_newest": True, "keep_raw_copy": False},
    {"enabled": True, "name": "MPlan Reg7", "remote_pattern": "MPlan_Reg7*ftp", "stable_local_name": "mPlanReg7.log", "destination_subfolder": "", "site_scope": "Both", "use_newest": True, "keep_raw_copy": False},
    {"enabled": True, "name": "MPlan Reg3", "remote_pattern": "MPlan_Reg3*ftp", "stable_local_name": "mPlanReg3.log", "destination_subfolder": "", "site_scope": "Both", "use_newest": True, "keep_raw_copy": False},
    {"enabled": True, "name": "MPlan Reg10", "remote_pattern": "MPlan_Reg10*ftp", "stable_local_name": "mPlanReg10.log", "destination_subfolder": "", "site_scope": "Both", "use_newest": True, "keep_raw_copy": False},
]


@dataclass(slots=True)
class TransferStatus:
    engine_state: str = "Stopped"
    active_site_id: str = "1"
    active_site_name: str = "Site 1"
    last_cycle_utc: str = ""
    last_success_utc: str = ""
    last_error: str = ""
    last_summary: str = ""
    last_mapping_results_json: str = "[]"
    is_running: bool = False


class TransferEngine:
    def __init__(self) -> None:
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._lock = threading.Lock()
        self._status = TransferStatus()
        self._recent_log_lines: list[str] = []

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                **asdict(self._status),
                "recent_log_lines": list(self._recent_log_lines[-200:]),
            }

    def start(self, config: dict[str, Any], active_site_id: str) -> bool:
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return False
            self._stop_event.clear()
            self._status.engine_state = "Starting"
            self._status.is_running = True
            self._status.active_site_id = active_site_id
            self._status.active_site_name = f"Site {active_site_id}"
        self._thread = threading.Thread(target=self._run_loop, args=(config, active_site_id), daemon=True)
        self._thread.start()
        self._append_log(f"Transfer engine starting for Site {active_site_id}.")
        return True

    def stop(self) -> None:
        self._stop_event.set()
        with self._lock:
            self._status.engine_state = "Stopping"
            self._status.is_running = False
        if self._thread is not None and self._thread.is_alive():
            self._thread.join(timeout=5)
        with self._lock:
            self._status.engine_state = "Stopped"
            self._status.is_running = False
        self._append_log("Transfer engine stopped.")

    def run_once(self, config: dict[str, Any], active_site_id: str) -> dict[str, Any]:
        return self._run_cycle(config, active_site_id)

    def _run_loop(self, config: dict[str, Any], active_site_id: str) -> None:
        interval = max(5, int(config.get("interval_seconds", 18) or 18))
        while not self._stop_event.is_set():
            try:
                self._run_cycle(config, active_site_id)
            except Exception as exc:  # noqa: BLE001
                logger.exception("Transfer cycle failed")
                self._set_error(str(exc))
            if self._stop_event.wait(interval):
                break
        with self._lock:
            self._status.engine_state = "Stopped"
            self._status.is_running = False

    def _run_cycle(self, config: dict[str, Any], active_site_id: str) -> dict[str, Any]:
        now = datetime.utcnow().isoformat()
        site_profiles = config.get("site_profiles", {})
        site_key = "site1" if str(active_site_id) == "1" else "site2"
        site = dict(site_profiles.get(site_key) or {})
        host = str(site.get("host", "")).strip()
        username = str(site.get("username", config.get("username", "pds"))).strip() or "pds"
        putty_path = Path(str(config.get("putty_path", "C:/Program Files/PuTTY/psftp.exe")))
        key_path = Path(str(config.get("key_path", "C:/Keys/pds.ppk")))
        script_root = Path(str(config.get("script_root", "C:/Console/Scripts")))
        files_script = str(config.get("files_script_name", "TransferDataFiles.ftp"))
        cycle_script = str(config.get("cycle_script_name", "TransferDataCycleToServer.ftp"))
        local_root = Path(str(site.get("local_root", site.get("destination_root", "")))).expanduser()
        if not host:
            raise RuntimeError(f"No host configured for {site_key}.")
        if not local_root:
            raise RuntimeError(f"No local root configured for {site_key}.")
        if not putty_path.exists():
            raise RuntimeError(f"psftp.exe not found: {putty_path}")
        if not key_path.exists():
            raise RuntimeError(f"PPK key not found: {key_path}")
        if not script_root.exists():
            raise RuntimeError(f"Script root not found: {script_root}")
        files_script_path = script_root / files_script
        cycle_script_path = script_root / cycle_script
        if not files_script_path.exists():
            raise RuntimeError(f"Files transfer script not found: {files_script_path}")
        if not cycle_script_path.exists():
            raise RuntimeError(f"Cycle transfer script not found: {cycle_script_path}")

        stage_root = local_root / "_transfer_stage" / site_key
        cycle_dir = stage_root / "cycle"
        eventlogs_dir = stage_root / "eventlogs"
        cycle_dir.mkdir(parents=True, exist_ok=True)
        eventlogs_dir.mkdir(parents=True, exist_ok=True)

        self._append_log(f"Running transfer cycle for {site_key} ({host})")
        self._run_psftp(putty_path, key_path, files_script_path, username, host, cycle_dir)
        self._run_psftp(putty_path, key_path, cycle_script_path, username, host, eventlogs_dir)

        mapping_results = self._apply_mappings(config.get("mappings", DEFAULT_MAPPINGS), active_site_id, local_root, [cycle_dir, eventlogs_dir])
        success_count = sum(1 for item in mapping_results if item.get("status") == "success")
        fail_count = sum(1 for item in mapping_results if item.get("status") != "success")
        summary = f"Cycle complete for Site {active_site_id}: {success_count} mapping(s) succeeded, {fail_count} failed."
        with self._lock:
            self._status.engine_state = "Running"
            self._status.active_site_id = str(active_site_id)
            self._status.active_site_name = f"Site {active_site_id}"
            self._status.last_cycle_utc = now
            self._status.last_success_utc = now
            self._status.last_error = ""
            self._status.last_summary = summary
            self._status.last_mapping_results_json = json.dumps(mapping_results)
            self._status.is_running = self._thread is not None and self._thread.is_alive()
        self._append_log(summary)
        return {"summary": summary, "mapping_results": mapping_results}

    def _run_psftp(self, putty_path: Path, key_path: Path, script_path: Path, username: str, host: str, cwd: Path) -> None:
        result = subprocess.run(
            [str(putty_path), "-i", str(key_path), "-v", "-b", str(script_path), f"{username}@{host}"],
            cwd=str(cwd),
            capture_output=True,
            text=True,
            timeout=120,
            check=False,
        )
        stdout = (result.stdout or "").strip()
        stderr = (result.stderr or "").strip()
        if stdout:
            self._append_log(stdout.splitlines()[-1])
        if result.returncode != 0:
            message = stderr or stdout or f"psftp returned code {result.returncode}"
            raise RuntimeError(message)

    def _apply_mappings(self, mappings: list[dict[str, Any]], active_site_id: str, local_root: Path, source_dirs: list[Path]) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        site_key = "site1" if str(active_site_id) == "1" else "site2"
        for mapping in mappings:
            enabled = bool(mapping.get("enabled", True))
            if not enabled:
                continue
            site_scope = str(mapping.get("site_scope", "Both") or "Both")
            if site_scope == "Site 1 only" and str(active_site_id) != "1":
                continue
            if site_scope == "Site 2 only" and str(active_site_id) != "2":
                continue
            pattern = str(mapping.get("remote_pattern", "")).strip()
            stable_name = str(mapping.get("stable_local_name", "")).strip()
            dest_subfolder = str(mapping.get("destination_subfolder", "")).strip()
            keep_raw_copy = bool(mapping.get("keep_raw_copy", False))
            use_newest = bool(mapping.get("use_newest", True))
            mapping_name = str(mapping.get("name", stable_name or pattern) or stable_name or pattern)
            if not pattern or not stable_name:
                results.append({"name": mapping_name, "status": "error", "message": "Pattern or stable filename is missing."})
                continue
            candidates: list[Path] = []
            for source_dir in source_dirs:
                if not source_dir.exists():
                    continue
                for file_path in source_dir.rglob("*"):
                    if file_path.is_file() and fnmatch.fnmatch(file_path.name, pattern):
                        candidates.append(file_path)
            if not candidates:
                results.append({"name": mapping_name, "status": "error", "message": f"No file matched {pattern}", "remote_pattern": pattern})
                continue
            selected = max(candidates, key=lambda path: path.stat().st_mtime) if use_newest else sorted(candidates, key=lambda path: path.name)[0]
            destination_root = local_root
            if dest_subfolder:
                destination_root = destination_root / dest_subfolder
            destination_root.mkdir(parents=True, exist_ok=True)
            destination_path = destination_root / stable_name
            shutil.copy2(selected, destination_path)
            if keep_raw_copy:
                raw_dir = local_root / "_transfer_raw"
                raw_dir.mkdir(parents=True, exist_ok=True)
                shutil.copy2(selected, raw_dir / selected.name)
            results.append(
                {
                    "name": mapping_name,
                    "status": "success",
                    "remote_pattern": pattern,
                    "matched_file": selected.name,
                    "destination": str(destination_path),
                    "site": site_key,
                }
            )
        return results

    def _set_error(self, message: str) -> None:
        now = datetime.utcnow().isoformat()
        with self._lock:
            self._status.engine_state = "Error"
            self._status.last_cycle_utc = now
            self._status.last_error = message
            self._status.last_summary = f"Transfer error: {message}"
        self._append_log(f"ERROR: {message}")

    def _append_log(self, line: str) -> None:
        stamped = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | {line}"
        with self._lock:
            self._recent_log_lines.append(stamped)
            self._recent_log_lines = self._recent_log_lines[-400:]
