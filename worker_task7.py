#!/usr/bin/env python3
"""
Worker Task 7 — Minimum clue analysis (parallel, dashboard-connected)
Usage: python worker_task7.py <worker_id> <total_workers>
  e.g. python worker_task7.py 0 15   (worker 0 of 15)

- Sends logs to dashboard via HTTP POST (visible in dashboard log panel)
- Saves checkpoint every 60s with resume support
- Verbose timing for diagnosis
"""
import sys
import os
import time
import random
import json
import urllib.request
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from engine import CheckpointManager, COLOR_MAP
from ortools.sat.python import cp_model

DASHBOARD_URL = "http://localhost:5000/api/worker_log"


def send_log(label, msg, level="info"):
    """Send log to dashboard + print locally."""
    line = f"[{label}] {msg}"
    print(line, flush=True)
    try:
        data = json.dumps({"task": "task7", "msg": line, "level": level}).encode()
        req = urllib.request.Request(DASHBOARD_URL, data=data,
                                     headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=2)
    except Exception:
        pass  # dashboard may be down — don't block worker


def has_unique_solution_9x9(clues, timeout=10):
    """Check if clues yield exactly 1 solution."""
    class Counter(cp_model.CpSolverSolutionCallback):
        def __init__(self):
            super().__init__()
            self.count = 0
        def on_solution_callback(self):
            self.count += 1
            if self.count >= 2:
                self.StopSearch()

    n, b = 9, 3
    model = cp_model.CpModel()
    digit = [[model.NewIntVar(0, n-1, f'd_{r}_{c}') for c in range(n)] for r in range(n)]
    color = [[model.NewIntVar(0, n-1, f'k_{r}_{c}') for c in range(n)] for r in range(n)]

    for r in range(n):
        model.AddAllDifferent(digit[r])
        model.AddAllDifferent(color[r])
    for c in range(n):
        model.AddAllDifferent([digit[r][c] for r in range(n)])
        model.AddAllDifferent([color[r][c] for r in range(n)])
    for br in range(b):
        for bc in range(b):
            model.AddAllDifferent([digit[r][c] for r in range(br*b,(br+1)*b) for c in range(bc*b,(bc+1)*b)])
            model.AddAllDifferent([color[r][c] for r in range(br*b,(br+1)*b) for c in range(bc*b,(bc+1)*b)])

    pair = [[model.NewIntVar(0, n*n-1, f'p_{r}_{c}') for c in range(n)] for r in range(n)]
    for r in range(n):
        for c in range(n):
            model.Add(pair[r][c] == digit[r][c] * n + color[r][c])
    model.AddAllDifferent([pair[r][c] for r in range(n) for c in range(n)])

    for (r, c), (d, k) in clues.items():
        model.Add(digit[r][c] == d)
        model.Add(color[r][c] == k)

    solver = cp_model.CpSolver()
    solver.parameters.enumerate_all_solutions = True
    solver.parameters.num_workers = 1
    solver.parameters.max_time_in_seconds = timeout
    counter = Counter()
    solver.Solve(model, counter)
    return counter.count


def greedy_removal_9x9(grid, label, n_shuffles=5, timeout_per_check=10):
    """Greedy removal with per-shuffle logging."""
    best_clues = None
    for shuffle_idx in range(n_shuffles):
        t_shuf = time.time()
        clues = {(r, c): grid[r][c] for r in range(9) for c in range(9)}
        cells = list(clues.keys())
        random.shuffle(cells)
        removed = 0
        for cell in cells:
            trial = dict(clues)
            del trial[cell]
            nsol = has_unique_solution_9x9(trial, timeout=timeout_per_check)
            if nsol == 1:
                clues = trial
                removed += 1

        dt_shuf = time.time() - t_shuf
        n_clues = len(clues)
        tag = f"  shuf {shuffle_idx+1}/{n_shuffles}: {n_clues} clues ({removed} removed) {dt_shuf:.0f}s"
        print(f"  [{label}] {tag}", flush=True)

        if best_clues is None or len(clues) < len(best_clues):
            best_clues = clues
    return best_clues


def main():
    if len(sys.argv) < 3:
        print("Usage: python worker_task7.py <worker_id> <total_workers>", flush=True)
        sys.exit(1)

    wid = int(sys.argv[1])
    n_workers = int(sys.argv[2])
    label = f"W{wid:02d}"

    send_log(label, f"Starting (worker {wid}/{n_workers})", "success")

    # ---- Load Task 3 ----
    ckpt3 = CheckpointManager("task3_search_grids")
    saved3 = ckpt3.load()
    if not saved3 or not saved3.get("orbits"):
        send_log(label, "ERROR: no Task 03 data", "error")
        sys.exit(1)

    orbits_dict = saved3["orbits"]
    all_hashes = list(orbits_dict.keys())
    my_hashes = all_hashes[wid::n_workers]

    if not my_hashes:
        send_log(label, f"No orbits to process (0/{len(all_hashes)})", "warning")
        sys.exit(0)

    send_log(label, f"Assigned {len(my_hashes)} / {len(all_hashes)} orbits")

    # ---- Parse grids ----
    orbit_grids = {}
    for chash in my_hashes:
        odata = orbits_dict[chash]
        gd = odata["grid"]
        grid_internal = tuple(
            tuple((cell["d"] - 1, COLOR_MAP[cell["c"]]) for cell in row)
            for row in gd
        )
        orbit_grids[chash] = grid_internal

    # ---- Resume checkpoint ----
    ckpt_name = f"task7_w{wid}"
    ckpt = CheckpointManager(ckpt_name)
    saved = ckpt.load()

    if saved and saved.get("status") == "done":
        send_log(label, f"Already DONE: {saved.get('processed',0)} orbits, "
                 f"min={saved.get('global_min','?')}. Skipping.", "success")
        return

    results = {}
    global_min = 81
    distribution = {}
    start_from = 0

    if saved and saved.get("status") == "running":
        results = saved.get("results", {})
        global_min = saved.get("global_min", 81)
        distribution = saved.get("distribution", {})
        my_hashes = [h for h in my_hashes if h not in results]
        start_from = len(results)
        send_log(label, f"Resuming: {start_from} done, {len(my_hashes)} remaining, min={global_min}", "warning")

    n_shuffles = 5
    t_start = time.time()
    last_save = time.time()
    total = start_from + len(my_hashes)

    for i, chash in enumerate(my_hashes):
        t_orbit = time.time()
        grid = orbit_grids[chash]

        best_clues = greedy_removal_9x9(grid, label, n_shuffles=n_shuffles,
                                         timeout_per_check=10)
        n_clues = len(best_clues)
        clue_cells = sorted(best_clues.keys())

        dt_orbit = time.time() - t_orbit

        dist_key = str(n_clues)
        distribution[dist_key] = distribution.get(dist_key, 0) + 1

        results[chash] = {
            "orbit_hash": chash,
            "n_clues": n_clues,
            "clue_cells": clue_cells,
        }

        done = start_from + i + 1
        elapsed = time.time() - t_start
        rate = (i + 1) / max(0.1, elapsed)
        remaining = len(my_hashes) - (i + 1)
        eta_min = (remaining / max(0.001, rate)) / 60

        if n_clues < global_min:
            global_min = n_clues
            send_log(label, f"{done}/{total} *** NEW MIN: {n_clues} clues *** "
                     f"({dt_orbit:.0f}s, ETA {eta_min:.0f}min)", "success")
        else:
            send_log(label, f"{done}/{total} -- {n_clues} clues (best:{global_min}) "
                     f"({dt_orbit:.0f}s, ETA {eta_min:.0f}min)")

        # Checkpoint every 60s
        if time.time() - last_save > 60:
            ckpt.save({
                "status": "running",
                "worker_id": wid, "n_workers": n_workers,
                "processed": done, "total": total,
                "global_min": global_min,
                "results": results,
                "distribution": distribution,
            })
            last_save = time.time()

    elapsed = round(time.time() - t_start, 1)

    result = {
        "status": "done",
        "worker_id": wid, "n_workers": n_workers,
        "processed": len(results), "total": total,
        "global_min": global_min,
        "results": results,
        "distribution": distribution,
        "elapsed": elapsed,
    }
    ckpt.save(result)

    send_log(label, f"DONE: {len(results)} orbits, min={global_min} clues, "
             f"in {elapsed:.0f}s ({elapsed/60:.1f}min)", "success")


if __name__ == "__main__":
    main()