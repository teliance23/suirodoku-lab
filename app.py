#!/usr/bin/env python3
"""
Suirodoku Lab -- Unified Research Dashboard v10
=================================================
Everything in 3 files: engine.py, app.py, dashboard.html.
Tasks 01-09 + Task 10 Hunter (a/b/c/d).
No config.py, no migrate.py, no import_hunter.py, no symmetry_hunter.py.
"""

import threading
import time
import json
import os
import subprocess
import sys
from flask import Flask, jsonify, send_from_directory, request
from pathlib import Path

from engine import (
    task1_exact_4x4, task2_orbit_analysis, task3_search_new_grids,
    task5_minimum_clue_4x4, task6_mate_count,
    task7_minimum_clue_9x9,
    CheckpointManager, TOTAL_STRUCTURAL, MODEL_GRID, COLOR_NAMES,
    grid_to_display, LOG, EXPORT_DIR,
    set_task3_target, get_task3_target,
    canon_lib_available,
)
from task10 import (task10_generate_worker, task10_catalog_worker,
                    task10_get_generation_summary, task10_get_catalog_summary,
                    task10_list_available_probes,
                    PROBE_TYPES as T10_PROBE_TYPES, _ckpt_name as t10_ckpt_name)
from task11 import task11_worker, task11_get_available_sources, task11_get_catalog_summary
from task12 import task12_worker, task12_get_summary
from task13 import (task13_catalog_worker, task13_generate_worker,
                    task13_get_generation_summary, task13_get_catalog_summary,
                    task13_list_available_probes, PROBE_TYPES, _ckpt_name)

# Pre-load C library in main thread to avoid race condition with worker threads
canon_lib_available()

app = Flask(__name__, static_folder="static")


# =============================================================
# Task State Management
# =============================================================

TASK_IDS = [
    "task1", "task2", "task3", "task5",
    "task6", "task7",
    "task10_g1", "task10_g2", "task10_g3", "task10_g4",
    "task10_g5", "task10_g6", "task10_g7", "task10_g8",
    "task10_g9", "task10_g10", "task10_g11", "task10_g12",
    "task10_g13", "task10_g14", "task10_g15", "task10_g16",
    "task10_w1", "task10_w2", "task10_w3", "task10_w4",
    "task10_w5", "task10_w6", "task10_w7", "task10_w8",
    "task11_w1", "task11_w2", "task11_w3", "task11_w4",
    "task11_w5", "task11_w6", "task11_w7", "task11_w8",
    "task12_w1", "task12_w2", "task12_w3", "task12_w4",
    "task12_w5", "task12_w6", "task12_w7", "task12_w8",
    "task13_w1", "task13_w2", "task13_w3", "task13_w4",
    "task13_w5", "task13_w6", "task13_w7", "task13_w8",
    "task13_g1", "task13_g2", "task13_g3", "task13_g4",
    "task13_g5", "task13_g6", "task13_g7", "task13_g8",
    "task13_g9", "task13_g10", "task13_g11", "task13_g12",
    "task13_g13", "task13_g14", "task13_g15", "task13_g16",
]

STOPPABLE_TASKS = [
    "task2", "task3", "task5", "task6", "task7",
    "task10_g1", "task10_g2", "task10_g3", "task10_g4",
    "task10_g5", "task10_g6", "task10_g7", "task10_g8",
    "task10_g9", "task10_g10", "task10_g11", "task10_g12",
    "task10_g13", "task10_g14", "task10_g15", "task10_g16",
    "task10_w1", "task10_w2", "task10_w3", "task10_w4",
    "task10_w5", "task10_w6", "task10_w7", "task10_w8",
    "task11_w1", "task11_w2", "task11_w3", "task11_w4",
    "task11_w5", "task11_w6", "task11_w7", "task11_w8",
    "task12_w1", "task12_w2", "task12_w3", "task12_w4",
    "task12_w5", "task12_w6", "task12_w7", "task12_w8",
    "task13_w1", "task13_w2", "task13_w3", "task13_w4",
    "task13_w5", "task13_w6", "task13_w7", "task13_w8",
    "task13_g1", "task13_g2", "task13_g3", "task13_g4",
    "task13_g5", "task13_g6", "task13_g7", "task13_g8",
    "task13_g9", "task13_g10", "task13_g11", "task13_g12",
    "task13_g13", "task13_g14", "task13_g15", "task13_g16",
]

CKPT_NAMES = {
    "task1": "task1_exact_4x4",
    "task2": "task2_orbit_analysis",
    "task3": "task3_search_grids",
    "task5": "task5_min_clues_4x4",
    "task6": "task6_mate_count",
    "task7": "task7_min_clues_9x9",
}


class TaskState:
    def __init__(self):
        self.lock = threading.Lock()
        self.tasks = {tid: {"status": "idle", "progress": {}, "result": None, "thread": None} for tid in TASK_IDS}
        self.stop_events = {tid: threading.Event() for tid in STOPPABLE_TASKS}
        self._load_checkpoints()

    def _load_checkpoints(self):
        for task_id, ckpt_name in CKPT_NAMES.items():
            ckpt = CheckpointManager(ckpt_name)
            saved = ckpt.load()
            if saved:
                status = saved.get("status", "idle")
                if status == "done":
                    if task_id == "task3" and saved.get("attempts", 0) < saved.get("max_attempts", 5000):
                        self.tasks[task_id]["status"] = "idle"
                        self.tasks[task_id]["result"] = saved
                        continue
                    self.tasks[task_id]["status"] = "done"
                    self.tasks[task_id]["result"] = saved
                elif status == "running":
                    self.tasks[task_id]["status"] = "paused"
                    self.tasks[task_id]["progress"] = saved

    def update_progress(self, task_id, progress):
        with self.lock:
            self.tasks[task_id]["progress"] = progress

    def get_state(self):
        with self.lock:
            out = {}
            for tid, t in self.tasks.items():
                out[tid] = {
                    "status": t["status"],
                    "progress": t["progress"],
                    "result": t["result"],
                }
                if out[tid]["result"]:
                    r = dict(out[tid]["result"])
                    # Strip large data from status endpoint
                    for key in ["found_grids", "known_hashes", "stabilizer_examples",
                                "best_puzzles", "mate_counts", "canon_map", "canon_forms",
                                "orbit_reps", "orbit_stabs", "best_v1", "best_v2",
                                "all_v1", "seen_digits", "results", "orbit_results",
                                "swap_canon_map", "merged_orbits",
                                "orbits", "orbit_counts", "orbit_sd", "orbit_grids",
                                "partition_results",
                                "mates", "canon_hashes", "stab_results", "sample",
                                "orbit_data"]:
                        if key in r:
                            if key == "found_grids":
                                r["found_grids_count"] = len(r[key])
                            r.pop(key, None)
                    if "orbit_details" in r:
                        r["orbit_details_count"] = len(r["orbit_details"])
                        r.pop("orbit_details", None)
                    out[tid]["result"] = r
            return out


state = TaskState()


# =============================================================
# Task Runners
# =============================================================

def _make_runner(task_id, task_func, **kwargs):
    def runner():
        if task_id in state.stop_events:
            state.stop_events[task_id].clear()
        state.tasks[task_id]["status"] = "running"
        def on_progress(p):
            state.update_progress(task_id, p)
        try:
            result = task_func(
                progress_callback=on_progress,
                stop_event=state.stop_events.get(task_id),
                **kwargs
            )
            if task_id in state.stop_events and state.stop_events[task_id].is_set():
                state.tasks[task_id]["status"] = "paused"
            else:
                state.tasks[task_id]["result"] = result
                state.tasks[task_id]["status"] = result.get("status", "done")
        except Exception as e:
            import traceback
            state.tasks[task_id]["status"] = "error"
            state.tasks[task_id]["progress"] = {"error": str(e), "message": f"CRASH: {e}"}
            LOG.add(task_id, f"CRASH: {e}", level="error")
            LOG.add(task_id, traceback.format_exc(), level="error")
    return runner


def run_task1():
    state.tasks["task1"]["status"] = "running"
    def on_progress(count):
        state.update_progress("task1", {"solutions_found": count})
    try:
        result = task1_exact_4x4(progress_callback=on_progress)
        state.tasks["task1"]["result"] = result
        state.tasks["task1"]["status"] = "done"
    except Exception as e:
        state.tasks["task1"]["status"] = "error"
        state.tasks["task1"]["progress"] = {"error": str(e)}


N_WORKERS = 15
_task7_selected = []
_task7_procs = []

def run_task7():
    global _task7_procs
    if "task7" in state.stop_events:
        state.stop_events["task7"].clear()
    state.tasks["task7"]["status"] = "running"

    worker_script = Path(__file__).parent / "worker_task7.py"
    work_dir = Path(__file__).parent
    ckpt_dir = work_dir / "checkpoints"

    # Clean old worker checkpoints
    for f in ckpt_dir.glob("task7_w*.json"):
        f.unlink()
    for f in ckpt_dir.glob("task7_w*.tmp"):
        f.unlink()

    LOG.add("task7", f"Launching {N_WORKERS} parallel workers...", level="success")

    # Launch workers
    _task7_procs = []
    log_dir = work_dir / "logs"
    log_dir.mkdir(exist_ok=True)
    _task7_logs = []
    for wid in range(N_WORKERS):
        log_file = open(log_dir / f"task7_w{wid}.log", "w")
        _task7_logs.append(log_file)
        p = subprocess.Popen(
            [sys.executable, str(worker_script), str(wid), str(N_WORKERS)],
            cwd=str(work_dir),
            stdout=log_file, stderr=log_file,
        )
        _task7_procs.append(p)

    LOG.add("task7", f"{N_WORKERS} workers started (PIDs: {[p.pid for p in _task7_procs]})", level="info")
    _task7_start = time.time()

    # Monitor loop
    try:
        while True:
            time.sleep(10)

            # Check stop
            if state.stop_events.get("task7") and state.stop_events["task7"].is_set():
                LOG.add("task7", "STOP requested — killing workers...", level="warning")
                for p in _task7_procs:
                    try:
                        p.terminate()
                    except Exception:
                        pass
                state.tasks["task7"]["status"] = "paused"
                _task7_procs = []
                return

            # Read worker checkpoints for progress
            total_done = 0
            total_orbits = 0
            global_min = 81
            merged_dist = {}
            n_alive = 0
            n_done_workers = 0

            for f in sorted(ckpt_dir.glob("task7_w*.json")):
                try:
                    with open(f) as fh:
                        d = json.load(fh)
                    processed = d.get("processed", 0)
                    total = d.get("total", 0)
                    wmin = d.get("global_min", 81)
                    total_done += processed
                    total_orbits += total
                    if wmin < global_min:
                        global_min = wmin
                    for k, v in d.get("distribution", {}).items():
                        merged_dist[k] = merged_dist.get(k, 0) + v
                    if d.get("status") == "done":
                        n_done_workers += 1
                except Exception:
                    pass

            for p in _task7_procs:
                if p.poll() is None:
                    n_alive += 1

            if total_orbits > 0 or n_alive > 0:
                state.update_progress("task7", {
                    "processed": total_done, "total": total_orbits,
                    "global_min": global_min,
                    "percent": round(100 * total_done / max(1, total_orbits), 1),
                    "distribution": merged_dist,
                    "workers": N_WORKERS,
                    "workers_alive": n_alive,
                    "workers_done": n_done_workers,
                })

            # Log dead workers
            if n_alive < N_WORKERS and n_alive + n_done_workers < N_WORKERS:
                n_crashed = N_WORKERS - n_alive - n_done_workers
                LOG.add("task7", f"WARNING: {n_crashed} workers crashed! {n_alive} alive, {n_done_workers} done. Check logs/ folder.", level="error")

            # All workers finished?
            if n_alive == 0 and time.time() - _task7_start > 120:
                LOG.add("task7", f"ALL WORKERS DONE: {total_done} orbits, min={global_min} clues", level="success")
                state.tasks["task7"]["status"] = "done"
                state.tasks["task7"]["result"] = {
                    "status": "done", "processed": total_done,
                    "total": total_orbits, "global_min": global_min,
                    "distribution": merged_dist,
                }
                _task7_procs = []
                return

    except Exception as e:
        LOG.add("task7", f"ERROR: {e}", level="error")
        state.tasks["task7"]["status"] = "error"
        for p in _task7_procs:
            try:
                p.terminate()
            except Exception:
                pass
        _task7_procs = []


_task10_gen_target = {}  # worker_id → n_target (unused now, kept for API compat)
_task10_probe_type = {}  # worker_id → probe_type
_gen_procs = {}  # tid → subprocess.Popen
_task13_probe_type = {}  # worker_id → probe_type (e.g. "standard", "r180")

def _make_gen_subprocess_runner(task_name, worker_id):
    """Create a runner that launches worker_gen.py as a subprocess.
    Each worker gets its own process → own GIL → 100% CPU per core."""
    tid = f"{task_name}_g{worker_id}"
    log_tag = task_name.replace("task", "")

    def runner():
        import signal
        if tid in state.stop_events:
            state.stop_events[tid].clear()
        state.tasks[tid]["status"] = "running"

        # Determine probe type and checkpoint name
        if task_name == "task13":
            probe_type = _task13_probe_type.get(worker_id, "standard")
            from task13 import _ckpt_suffix
            ckpt_name = f"task13_g{worker_id}_{_ckpt_suffix(probe_type)}"
        elif task_name == "task10":
            probe_type = _task10_probe_type.get(worker_id, "standard")
            from task10 import _ckpt_suffix as t10_ckpt_suffix
            ckpt_name = f"task10_g{worker_id}_{t10_ckpt_suffix(probe_type)}"
        else:
            probe_type = "standard"
            ckpt_name = f"{task_name}_g{worker_id}_bc"

        # Check if already done
        ckpt = CheckpointManager(ckpt_name)
        saved = ckpt.load()
        if saved and saved.get("status") == "done":
            n = saved.get("n_found", saved.get("n_mates_breaking", 0) * 9)
            state.tasks[tid]["result"] = saved
            state.tasks[tid]["status"] = "done"
            state.update_progress(tid, {"phase": "done", "percent": 100,
                "n_found": n, "message": f"Already done: {n:,} mates"})
            LOG.add(task_name, f"[G{worker_id}] Already done: {n:,}", level="info")
            return

        worker_script = Path(__file__).parent / "worker_gen.py"
        log_dir = Path(__file__).parent / "logs"
        log_dir.mkdir(exist_ok=True)
        log_file = open(log_dir / f"{tid}.log", "w")

        LOG.add(task_name, f"[G{worker_id}] Launching subprocess (probe={probe_type})...", level="info")
        env = os.environ.copy()
        env["PROBE_TYPE"] = probe_type
        proc = subprocess.Popen(
            [sys.executable, str(worker_script), task_name, str(worker_id)],
            cwd=str(Path(__file__).parent),
            stdout=log_file, stderr=log_file,
            env=env,
        )
        _gen_procs[tid] = proc
        LOG.add(task_name, f"[G{worker_id}] PID={proc.pid}", level="success")

        progress_file = ckpt.filepath.parent / f"{ckpt_name}_progress.json"

        # Monitor loop
        last_log = [0]
        last_n = [0]
        try:
            while proc.poll() is None:
                time.sleep(2)
                # Check stop
                if state.stop_events.get(tid) and state.stop_events[tid].is_set():
                    LOG.add(task_name, f"[G{worker_id}] STOP requested — writing stop file...", level="warning")
                    # Write stop file for graceful shutdown (cross-platform, works on Windows)
                    stop_file = ckpt.filepath.parent / f"{ckpt_name}_stop"
                    try:
                        stop_file.write_text("stop")
                    except Exception as e:
                        LOG.add(task_name, f"[G{worker_id}] Failed to write stop file: {e}", level="error")
                    # Wait for process to exit gracefully (worker detects file, saves, exits)
                    try:
                        proc.wait(timeout=60)
                        LOG.add(task_name, f"[G{worker_id}] Graceful stop OK", level="success")
                    except subprocess.TimeoutExpired:
                        LOG.add(task_name, f"[G{worker_id}] Timeout — killing PID={proc.pid}", level="warning")
                        proc.kill()
                        proc.wait(timeout=10)
                    # Clean up stop file
                    try:
                        stop_file.unlink(missing_ok=True)
                    except Exception:
                        pass
                    state.tasks[tid]["status"] = "paused"
                    # Read last progress
                    try:
                        with open(progress_file) as f:
                            prog = json.load(f)
                        state.tasks[tid]["result"] = prog
                        state.update_progress(tid, {
                            "n_found": prog.get("n_found", 0),
                            "rate": prog.get("rate", 0),
                            "message": f"⏸ {prog.get('n_found', 0):,} mates"
                        })
                    except:
                        pass
                    return

                # Read progress file
                try:
                    with open(progress_file) as f:
                        prog = json.load(f)
                    n_found = prog.get("n_found", 0)
                    rate = prog.get("rate", 0)
                    elapsed = prog.get("elapsed", 0)
                    state.update_progress(tid, {
                        "phase": "gen",
                        "n_found": n_found,
                        "rate": rate,
                        "message": f"{n_found:,} mates — {rate}/s",
                    })
                    # Log periodically (every 30s) if progress changed
                    now = time.time()
                    if n_found > last_n[0] and (now - last_log[0] > 30):
                        LOG.add(task_name, f"[G{worker_id}] {n_found:,} mates — "
                                f"{rate}/s ({elapsed:.0f}s)", level="math")
                        last_log[0] = now
                        last_n[0] = n_found
                except:
                    pass

            # Process exited
            rc = proc.returncode
            log_file.close()
            _gen_procs.pop(tid, None)

            if rc == 0:
                # Read final checkpoint
                saved = ckpt.load()
                if saved:
                    state.tasks[tid]["result"] = saved
                    n = saved.get("n_found", 0)
                    state.tasks[tid]["status"] = "done"
                    state.update_progress(tid, {
                        "phase": "done", "percent": 100,
                        "n_found": n, "rate": saved.get("rate", 0),
                        "message": f"✅ {n:,} mates"
                    })
                    LOG.add(task_name, f"[G{worker_id}] DONE: {n:,} mates", level="success")
                else:
                    state.tasks[tid]["status"] = "error"
                    state.tasks[tid]["progress"] = {"message": "No checkpoint after completion"}
            else:
                state.tasks[tid]["status"] = "error"
                msg = f"Process exited with code {rc}"
                state.tasks[tid]["progress"] = {"error": msg, "message": msg}
                LOG.add(task_name, f"[G{worker_id}] {msg}", level="error")
                # Read last log lines
                try:
                    with open(log_dir / f"{tid}.log") as f:
                        lines = f.readlines()[-5:]
                    LOG.add(task_name, f"[G{worker_id}] Last output: {''.join(lines)}", level="error")
                except:
                    pass

        except Exception as e:
            import traceback
            state.tasks[tid]["status"] = "error"
            state.tasks[tid]["progress"] = {"error": str(e), "message": f"CRASH: {e}"}
            LOG.add(task_name, f"[G{worker_id}] CRASH: {e}", level="error")
            LOG.add(task_name, traceback.format_exc(), level="error")
            if proc.poll() is None:
                proc.kill()
    return runner

def _make_task10_gen_runner(worker_id):
    return _make_gen_subprocess_runner("task10", worker_id)

_task10_cat_probes = {}  # worker_id → list of probe_types

def _make_task10_cat_runner(worker_id):
    tid = f"task10_w{worker_id}"
    def runner():
        if tid in state.stop_events:
            state.stop_events[tid].clear()
        state.tasks[tid]["status"] = "running"
        def on_progress(p):
            state.update_progress(tid, p)
        probe_types = _task10_cat_probes.get(worker_id, ["standard"])
        try:
            result = task10_catalog_worker(
                worker_id,
                progress_callback=on_progress,
                stop_event=state.stop_events.get(tid),
                probe_types=probe_types,
            )
            if state.stop_events.get(tid) and state.stop_events[tid].is_set():
                state.tasks[tid]["status"] = "paused"
                state.tasks[tid]["result"] = result
            else:
                state.tasks[tid]["result"] = result
                st = result.get("status", "done")
                state.tasks[tid]["status"] = st
                if st == "error":
                    msg = result.get("message", "Unknown error")
                    state.tasks[tid]["progress"] = {"error": msg, "message": msg}
        except Exception as e:
            import traceback
            state.tasks[tid]["status"] = "error"
            state.tasks[tid]["progress"] = {"error": str(e), "message": f"CRASH: {e}"}
            LOG.add("task10", f"[C{worker_id}] CRASH: {e}", level="error")
            LOG.add("task10", traceback.format_exc(), level="error")
    return runner

RUNNERS = {
    "task1": run_task1,
    "task2": _make_runner("task2", task2_orbit_analysis),
    "task3": _make_runner("task3", task3_search_new_grids),
    "task5": _make_runner("task5", task5_minimum_clue_4x4),
    "task6": _make_runner("task6", task6_mate_count),
    "task7": run_task7,
}
for _w in range(1, 17):
    RUNNERS[f"task10_g{_w}"] = _make_task10_gen_runner(_w)
for _w in range(1, 9):
    RUNNERS[f"task10_w{_w}"] = _make_task10_cat_runner(_w)

_task11_sources = {}  # worker_id -> source_name, e.g. {"w1": "10", "w2": "10i"}

def _make_task11_runner(worker_id):
    """Create a runner function for task11 worker N."""
    tid = f"task11_w{worker_id}"
    def runner():
        source = _task11_sources.get(f"w{worker_id}", "10")
        if tid in state.stop_events:
            state.stop_events[tid].clear()
        state.tasks[tid]["status"] = "running"
        def on_progress(p):
            state.update_progress(tid, p)
        try:
            result = task11_worker(
                worker_id, source,
                progress_callback=on_progress,
                stop_event=state.stop_events.get(tid),
            )
            if state.stop_events.get(tid) and state.stop_events[tid].is_set():
                state.tasks[tid]["status"] = "paused"
                state.tasks[tid]["result"] = result
            else:
                state.tasks[tid]["result"] = result
                st = result.get("status", "done")
                state.tasks[tid]["status"] = st
                if st == "error":
                    msg = result.get("message", "Unknown error")
                    state.tasks[tid]["progress"] = {"error": msg, "message": msg}
        except Exception as e:
            import traceback
            state.tasks[tid]["status"] = "error"
            state.tasks[tid]["progress"] = {"error": str(e), "message": f"CRASH: {e}"}
            LOG.add("task11", f"[W{worker_id}] CRASH: {e}", level="error")
            LOG.add("task11", traceback.format_exc(), level="error")
    return runner

for _w in range(1, 9):
    RUNNERS[f"task11_w{_w}"] = _make_task11_runner(_w)

_task12_n_shuffles = {}  # worker_id → n_shuffles

def _make_task12_runner(worker_id):
    tid = f"task12_w{worker_id}"
    def runner():
        if tid in state.stop_events:
            state.stop_events[tid].clear()
        state.tasks[tid]["status"] = "running"
        n_shuffles = _task12_n_shuffles.get(worker_id, 50)
        def on_progress(p):
            state.update_progress(tid, p)
        try:
            result = task12_worker(
                worker_id, n_shuffles=n_shuffles,
                progress_callback=on_progress,
                stop_event=state.stop_events.get(tid),
            )
            if state.stop_events.get(tid) and state.stop_events[tid].is_set():
                state.tasks[tid]["status"] = "paused"
                state.tasks[tid]["result"] = result
            else:
                state.tasks[tid]["result"] = result
                st = result.get("status", "done")
                state.tasks[tid]["status"] = st
                if st == "error":
                    msg = result.get("message", "Unknown error")
                    state.tasks[tid]["progress"] = {"error": msg, "message": msg}
        except Exception as e:
            import traceback
            state.tasks[tid]["status"] = "error"
            state.tasks[tid]["progress"] = {"error": str(e), "message": f"CRASH: {e}"}
            LOG.add("task12", f"[W{worker_id}] CRASH: {e}", level="error")
            LOG.add("task12", traceback.format_exc(), level="error")
    return runner

for _w in range(1, 9):
    RUNNERS[f"task12_w{_w}"] = _make_task12_runner(_w)

_task13_cat_probes = {}   # worker_id → list of probe_types

def _make_task13_runner(worker_id):
    """Create a runner function for task13 catalog worker N."""
    tid = f"task13_w{worker_id}"
    def runner():
        if tid in state.stop_events:
            state.stop_events[tid].clear()
        state.tasks[tid]["status"] = "running"
        def on_progress(p):
            state.update_progress(tid, p)
        probe_types = _task13_cat_probes.get(worker_id, ["standard"])
        try:
            result = task13_catalog_worker(
                worker_id,
                progress_callback=on_progress,
                stop_event=state.stop_events.get(tid),
                probe_types=probe_types,
            )
            if state.stop_events.get(tid) and state.stop_events[tid].is_set():
                state.tasks[tid]["status"] = "paused"
                state.tasks[tid]["result"] = result
            else:
                state.tasks[tid]["result"] = result
                st = result.get("status", "done")
                state.tasks[tid]["status"] = st
                if st == "error":
                    msg = result.get("message", "Unknown error")
                    state.tasks[tid]["progress"] = {"error": msg, "message": msg}
        except Exception as e:
            import traceback
            state.tasks[tid]["status"] = "error"
            state.tasks[tid]["progress"] = {"error": str(e), "message": f"CRASH: {e}"}
            LOG.add("task13", f"[W{worker_id}] CRASH: {e}", level="error")
            LOG.add("task13", traceback.format_exc(), level="error")
    return runner

for _w in range(1, 9):
    RUNNERS[f"task13_w{_w}"] = _make_task13_runner(_w)

_task13_gen_target = {}  # worker_id → n_target (unused, kept for API compat)

def _make_task13_gen_runner(worker_id):
    return _make_gen_subprocess_runner("task13", worker_id)

for _w in range(1, 17):
    RUNNERS[f"task13_g{_w}"] = _make_task13_gen_runner(_w)


# =============================================================
# Routes — Dashboard
# =============================================================

@app.route("/")
def index():
    return send_from_directory("static", "dashboard.html")


# =============================================================
# Routes — Task API
# =============================================================

@app.route("/api/status")
def api_status():
    return jsonify(state.get_state())

@app.route("/api/logs")
def api_logs():
    n = request.args.get("n", 100, type=int)
    task = request.args.get("task", None)
    entries = LOG.get_recent(n)
    if task:
        entries = [e for e in entries if e["task"] == task or e["task"].startswith(task)]
    return jsonify(entries)

@app.route("/api/start/<task_id>", methods=["POST"])
def api_start(task_id):
    global _task7_selected
    if task_id not in RUNNERS:
        LOG.add("app", f"Unknown task_id: {task_id}", level="error")
        return jsonify({"error": "Unknown task"}), 400

    # Fix stuck tasks: if status is "running" but thread is dead, force idle
    current_status = state.tasks[task_id]["status"]
    if current_status == "running":
        thread = state.tasks[task_id].get("thread")
        if thread is None or not thread.is_alive():
            state.tasks[task_id]["status"] = "idle"
            current_status = "idle"
        else:
            return jsonify({"error": "Already running"}), 400

    LOG.add("app", f"▶ Starting {task_id}...", level="info")

    if task_id == "task7":
        data = request.get_json(silent=True) or {}
        _task7_selected = data.get("selected", [])
    if task_id.startswith("task10_g"):
        data = request.get_json(silent=True) or {}
        wid = int(task_id.replace("task10_g", ""))
        probe_type = data.get("probe_type", "standard")
        _task10_gen_target[wid] = data.get("n_target", 100_000)
        _task10_probe_type[wid] = probe_type
        LOG.add("task10", f"[G{wid}] probe={probe_type}", level="info")
    if task_id.startswith("task10_w"):
        data = request.get_json(silent=True) or {}
        wid = int(task_id.replace("task10_w", ""))
        probe_types = data.get("probe_types", ["standard"])
        _task10_cat_probes[wid] = probe_types
    if task_id.startswith("task13_g"):
        data = request.get_json(silent=True) or {}
        wid = int(task_id.replace("task13_g", ""))
        n_target = data.get("n_target", 100_000)
        probe_type = data.get("probe_type", "standard")
        _task13_gen_target[wid] = n_target
        _task13_probe_type[wid] = probe_type
        LOG.add("task13", f"[G{wid}] probe={probe_type}, n_target={n_target}", level="info")
    if task_id.startswith("task13_w"):
        data = request.get_json(silent=True) or {}
        wid = int(task_id.replace("task13_w", ""))
        probe_types = data.get("probe_types", ["standard"])
        _task13_cat_probes[wid] = probe_types
    if task_id.startswith("task12_w"):
        data = request.get_json(silent=True) or {}
        wid = int(task_id.replace("task12_w", ""))
        _task12_n_shuffles[wid] = data.get("n_shuffles", 50)
    if task_id.startswith("task11_w"):
        data = request.get_json(silent=True) or {}
        wid = int(task_id.replace("task11_w", ""))
        src = data.get("source", "10")
        _task11_sources[f"w{wid}"] = src
    if current_status == "done":
        state.tasks[task_id]["status"] = "idle"
    t = threading.Thread(target=RUNNERS[task_id], daemon=True)
    state.tasks[task_id]["thread"] = t
    t.start()
    return jsonify({"ok": True})

@app.route("/api/stop/<task_id>", methods=["POST"])
def api_stop(task_id):
    if task_id in state.stop_events:
        state.stop_events[task_id].set()
        return jsonify({"ok": True})
    return jsonify({"error": "Cannot stop"}), 400

@app.route("/api/reset/<task_id>", methods=["POST"])
def api_reset(task_id):
    # Stop if running
    if task_id in state.stop_events:
        state.stop_events[task_id].set()
    time.sleep(0.3)

    # Clear checkpoint based on task type
    cleared = False
    if task_id in CKPT_NAMES:
        CheckpointManager(CKPT_NAMES[task_id]).clear()
        cleared = True
    elif task_id.startswith("task10_g"):
        wid = task_id.replace("task10_g", "")
        for pt in T10_PROBE_TYPES:
            CheckpointManager(t10_ckpt_name(int(wid), pt)).clear()
        cleared = True
    elif task_id.startswith("task10_w"):
        # Catalog workers don't have individual checkpoints
        cleared = True
    elif task_id.startswith("task12_w"):
        wid = task_id.replace("task12_w", "")
        CheckpointManager(f"task12_w{wid}").clear()
        cleared = True
    elif task_id.startswith("task13_g"):
        wid = task_id.replace("task13_g", "")
        # Clear checkpoints for all probe types
        for pt in PROBE_TYPES:
            CheckpointManager(_ckpt_name(int(wid), pt)).clear()
        cleared = True
    elif task_id.startswith("task13_w"):
        # Catalog workers don't have individual checkpoints
        cleared = True

    if cleared or task_id in state.tasks:
        state.tasks[task_id] = {"status": "idle", "progress": {}, "result": None, "thread": None}
        LOG.add("app", f"↺ Reset {task_id}", level="info")
        return jsonify({"ok": True})
    return jsonify({"error": "Unknown"}), 400

@app.route("/api/model_grid")
def api_model_grid():
    return jsonify(grid_to_display(MODEL_GRID))

@app.route("/api/grids")
def api_grids():
    ckpt = CheckpointManager("task3_search_grids")
    saved = ckpt.load()
    if saved and "found_grids" in saved:
        page = request.args.get("page", 0, type=int)
        per_page = request.args.get("per_page", 5, type=int)
        grids = saved["found_grids"]
        total = len(grids)
        start = page * per_page
        end = min(start + per_page, total)
        return jsonify({"total": total, "page": page, "per_page": per_page, "grids": grids[start:end]})
    return jsonify({"total": 0, "grids": []})

@app.route("/api/task7/orbits")
def api_task7_orbits():
    ckpt3 = CheckpointManager("task3_search_grids")
    saved3 = ckpt3.load()
    if not saved3 or not saved3.get("orbits"):
        return jsonify({"orbits": [], "error": "Run Task 03 first"})

    orbits_dict = saved3["orbits"]

    ckpt7 = CheckpointManager("task7_min_clues_9x9")
    saved7 = ckpt7.load()
    t7_results = saved7.get("results", {}) if saved7 else {}
    if isinstance(t7_results, list):
        t7_results = {}

    orbits = []
    for idx, (chash, odata) in enumerate(orbits_dict.items()):
        entry = {
            "index": idx + 1,
            "hash": chash,
            "n_grids": 1,
            "stab_size": odata.get("stab_size"),
            "symmetries": odata.get("symmetry_types", [])[:5],
            "source": odata.get("source", "solver"),
        }
        if chash in t7_results:
            entry["min_clues"] = t7_results[chash]["n_clues"]
        else:
            entry["min_clues"] = None
        orbits.append(entry)

    return jsonify({"orbits": orbits, "total": len(orbits)})

@app.route("/api/task3/target", methods=["GET", "POST"])
def api_task3_target():
    if request.method == "POST":
        data = request.get_json() or {}
        new_target = data.get("target", 5000)
        actual = set_task3_target(new_target)
        return jsonify({"target": actual})
    return jsonify({"target": get_task3_target()})


# --- Task 11 API ---

@app.route("/api/task10/summary")
def api_task10_summary():
    probe = request.args.get("probe", "standard")
    return jsonify(task10_get_generation_summary(probe))

@app.route("/api/task10/summary/<probe_type>")
def api_task10_summary_probe(probe_type):
    if probe_type not in T10_PROBE_TYPES:
        return jsonify({"error": f"Unknown probe: {probe_type}"}), 400
    return jsonify(task10_get_generation_summary(probe_type))

@app.route("/api/task10/catalog")
def api_task10_catalog():
    probe = request.args.get("probe", "standard")
    return jsonify(task10_get_catalog_summary(probe))

@app.route("/api/task10/catalog/<probe_type>")
def api_task10_catalog_probe(probe_type):
    if probe_type not in T10_PROBE_TYPES:
        return jsonify({"error": f"Unknown probe: {probe_type}"}), 400
    return jsonify(task10_get_catalog_summary(probe_type))

@app.route("/api/task10/probes")
def api_task10_probes():
    return jsonify(task10_list_available_probes())

@app.route("/api/task11/sources")
def api_task11_sources():
    return jsonify(task11_get_available_sources())

@app.route("/api/task11/catalog")
def api_task11_catalog():
    return jsonify(task11_get_catalog_summary())

@app.route("/api/task12/summary")
def api_task12_summary():
    return jsonify(task12_get_summary())

@app.route("/api/task13/summary")
def api_task13_summary():
    """Generation summary — supports ?probe=r180 etc."""
    probe = request.args.get("probe", "standard")
    return jsonify(task13_get_generation_summary(probe))

@app.route("/api/task13/summary/<probe_type>")
def api_task13_summary_probe(probe_type):
    """Generation summary for a specific probe type."""
    if probe_type not in PROBE_TYPES:
        return jsonify({"error": f"Unknown probe: {probe_type}"}), 400
    return jsonify(task13_get_generation_summary(probe_type))

@app.route("/api/task13/catalog")
def api_task13_catalog():
    """Catalog summary — supports ?probe=r180 etc."""
    probe = request.args.get("probe", "standard")
    return jsonify(task13_get_catalog_summary(probe))

@app.route("/api/task13/catalog/<probe_type>")
def api_task13_catalog_probe(probe_type):
    """Catalog summary for a specific probe type."""
    if probe_type not in PROBE_TYPES:
        return jsonify({"error": f"Unknown probe: {probe_type}"}), 400
    return jsonify(task13_get_catalog_summary(probe_type))

@app.route("/api/task13/probes")
def api_task13_probes():
    """List all probe types and their availability status."""
    return jsonify(task13_list_available_probes())

@app.route("/api/exports/<filename>")
def api_export_file(filename):
    return send_from_directory(str(EXPORT_DIR), filename)


@app.route("/api/perf")
def api_perf():
    """Return all perf log files for task10 and task13 generation workers.
    Supports ?probe=r180 etc. to select which probe's perf data."""
    ckpt_dir = Path(__file__).parent / "checkpoints"
    probe = request.args.get("probe", "standard")
    result = {}

    # Task 10: probe-specific perf files
    from task10 import _ckpt_suffix as t10_suffix
    t10s = t10_suffix(probe)
    workers10 = {}
    for wid in range(1, 17):
        perf_file = ckpt_dir / f"task10_g{wid}_{t10s}_perf.json"
        if perf_file.exists():
            try:
                with open(perf_file) as f:
                    workers10[f"G{wid}"] = json.load(f)
            except Exception:
                pass
    if workers10:
        result["task10"] = workers10

    # Task 13: probe-specific perf files
    from task13 import _ckpt_suffix
    suffix = _ckpt_suffix(probe)
    workers13 = {}
    for wid in range(1, 17):
        perf_file = ckpt_dir / f"task13_g{wid}_{suffix}_perf.json"
        if perf_file.exists():
            try:
                with open(perf_file) as f:
                    workers13[f"G{wid}"] = json.load(f)
            except Exception:
                pass
    if workers13:
        result["task13"] = workers13

    # List which probes have perf data for each task
    for task_prefix, suffix_fn in [("task10", t10_suffix), ("task13", _ckpt_suffix)]:
        available_probes = []
        for pt in PROBE_TYPES:
            pt_suffix = suffix_fn(pt)
            for wid in range(1, 17):
                if (ckpt_dir / f"{task_prefix}_g{wid}_{pt_suffix}_perf.json").exists():
                    available_probes.append(pt)
                    break
        result[f"{task_prefix}_probes_with_perf"] = available_probes

    return jsonify(result)


# =============================================================
# Routes — Worker Integration (Task 7 parallel)
# =============================================================

@app.route("/api/worker_log", methods=["POST"])
def api_worker_log():
    """Receive log entries from external workers."""
    data = request.get_json(silent=True)
    if not data:
        return jsonify({"error": "no data"}), 400
    LOG.add(
        data.get("task", "task7"),
        data.get("msg", ""),
        level=data.get("level", "info"),
    )
    return jsonify({"ok": True})


@app.route("/api/task7/workers")
def api_task7_workers():
    """Aggregate progress from all task7 worker checkpoints."""
    ckpt_dir = Path(__file__).parent / "checkpoints"
    workers = []
    total_done = 0
    total_orbits = 0
    global_min = 81
    merged_dist = {}

    for f in sorted(ckpt_dir.glob("task7_w*.json")):
        try:
            with open(f) as fh:
                d = json.load(fh)
            wid = d.get("worker_id", "?")
            processed = d.get("processed", 0)
            total = d.get("total", 0)
            wmin = d.get("global_min", 81)
            status = d.get("status", "unknown")
            workers.append({
                "worker_id": wid, "processed": processed,
                "total": total, "global_min": wmin, "status": status,
            })
            total_done += processed
            total_orbits += total
            if wmin < global_min:
                global_min = wmin
            for k, v in d.get("distribution", {}).items():
                merged_dist[k] = merged_dist.get(k, 0) + v
        except Exception:
            pass

    if total_orbits > 0:
        state.tasks["task7"]["progress"] = {
            "processed": total_done, "total": total_orbits,
            "global_min": global_min,
            "percent": round(100 * total_done / total_orbits, 1),
            "distribution": merged_dist,
            "workers": len(workers),
        }
        if total_done >= total_orbits:
            state.tasks["task7"]["status"] = "done"
        elif total_done > 0:
            state.tasks["task7"]["status"] = "running"

    return jsonify({
        "workers": workers,
        "total_done": total_done, "total_orbits": total_orbits,
        "global_min": global_min, "distribution": merged_dist,
    })


# =============================================================
# Entry point
# =============================================================

if __name__ == "__main__":
    LOG.add("app", "🚀 Suirodoku Lab v12 starting — LOG system OK", level="success")
    LOG.add("app", f"Tasks: {len(TASK_IDS)} registered, {len(RUNNERS)} runners, "
            f"{len(STOPPABLE_TASKS)} stoppable", level="info")
    print()
    print("  +============================================+")
    print("  |        SUIRODOKU LAB v12                   |")
    print("  |        Unified Research Dashboard          |")
    print("  +============================================+")
    print(f"  |  Tasks: 01-07, 10, 12, 13")
    print(f"  |  Runners: {len(RUNNERS)}")
    print(f"  |  http://localhost:5000")
    print("  +============================================+")
    print()
    app.run(host="0.0.0.0", port=5000, debug=False, threaded=True)