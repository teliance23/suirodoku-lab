#!/usr/bin/env python3
"""
worker_gen.py — Subprocess worker for task10/task13 generation.
Launched by app.py via _make_gen_subprocess_runner.

Usage: python worker_gen.py <task_name> <worker_id>
  e.g. python worker_gen.py task13 3

Environment variables:
  PROBE_TYPE  — symmetry probe type: standard, r180, order3, c3inv, c4trans

Graceful stop mechanism:
  app.py writes a file  checkpoints/<ckpt_name>_stop
  This worker polls for that file every 2s via progress callback.
  When detected: sets stop_event → worker saves reservoir → exits cleanly.
  The stop file is deleted on exit.
"""
import sys
import os
import signal
import time
import json
import threading
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from engine import CheckpointManager

# ── Global stop event shared with the generation worker ──
_stop_event = threading.Event()
_stop_file_path = None  # set in main()


def _check_stop_file():
    """Check if the stop file exists (written by app.py). Thread-safe."""
    if _stop_file_path and _stop_file_path.exists():
        return True
    return False


def _cleanup_stop_file():
    """Remove the stop file on exit."""
    try:
        if _stop_file_path and _stop_file_path.exists():
            _stop_file_path.unlink()
    except Exception:
        pass


def _handle_terminate(signum, frame):
    """Handle SIGTERM (Linux) / SIGBREAK (Windows)."""
    print(f"[worker_gen] Signal {signum} received — requesting graceful stop...", flush=True)
    _stop_event.set()


def write_progress(progress_file, data):
    """Write progress to JSON file (atomic via tmp)."""
    try:
        tmp = progress_file.with_suffix('.tmp')
        with open(tmp, 'w') as f:
            json.dump(data, f)
        tmp.replace(progress_file)
    except Exception as e:
        print(f"[worker_gen] Progress write error: {e}", flush=True)


def main():
    global _stop_file_path

    if len(sys.argv) < 3:
        print("Usage: python worker_gen.py <task_name> <worker_id>", flush=True)
        sys.exit(1)

    task_name = sys.argv[1]
    worker_id = int(sys.argv[2])
    probe_type = os.environ.get("PROBE_TYPE", "standard")

    # ── Install signal handlers (works on Linux; on Windows SIGTERM = kill, but try anyway) ──
    signal.signal(signal.SIGTERM, _handle_terminate)
    if hasattr(signal, 'SIGBREAK'):
        signal.signal(signal.SIGBREAK, _handle_terminate)

    print(f"[worker_gen] task={task_name}, worker={worker_id}, probe={probe_type}", flush=True)

    if task_name == "task10":
        from task10 import task10_generate_worker, _ckpt_suffix
        ckpt_name = f"task10_g{worker_id}_{_ckpt_suffix(probe_type)}"
    elif task_name == "task13":
        from task13 import task13_generate_worker, _ckpt_suffix
        ckpt_name = f"task13_g{worker_id}_{_ckpt_suffix(probe_type)}"
    else:
        print(f"[worker_gen] Unknown task: {task_name}", flush=True)
        sys.exit(1)

    ckpt = CheckpointManager(ckpt_name)
    progress_file = Path(ckpt.filepath).parent / f"{ckpt_name}_progress.json"

    # ── Stop file: app.py will create this file to request graceful stop ──
    _stop_file_path = Path(ckpt.filepath).parent / f"{ckpt_name}_stop"
    _cleanup_stop_file()  # remove leftover from previous run

    _last_stop_check = [0.0]

    def on_progress(data):
        write_progress(progress_file, data)
        # Poll for stop file every ~2s (not on every callback to avoid I/O spam)
        now = time.time()
        if not _stop_event.is_set() and (now - _last_stop_check[0]) >= 2.0:
            _last_stop_check[0] = now
            if _check_stop_file():
                print(f"[worker_gen] Stop file detected — requesting graceful stop...", flush=True)
                _stop_event.set()

    # ── Launch the worker ──
    if task_name == "task10":
        result = task10_generate_worker(
            worker_id, n_target=0,
            progress_callback=on_progress,
            stop_event=_stop_event,
            probe_type=probe_type,
        )
    elif task_name == "task13":
        result = task13_generate_worker(
            worker_id, n_target=0,
            progress_callback=on_progress,
            stop_event=_stop_event,
            probe_type=probe_type,
        )

    _cleanup_stop_file()
    print(f"[worker_gen] DONE: {result}", flush=True)


if __name__ == "__main__":
    main()
