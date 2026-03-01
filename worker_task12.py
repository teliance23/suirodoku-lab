#!/usr/bin/env python3
"""
Worker Task 12 — Crystal Grid Minimum Clues (proven)
Usage: python worker_task12.py <worker_id> <total_workers> [n_shuffles]
  e.g. python worker_task12.py 0 8 200   (worker 0 of 8, 200 shuffles)

Phase 1: Greedy removal with many shuffles → upper bound
Phase 2: Criticality check — prove every clue is necessary
Phase 3: Try to beat (best-1) by exhaustive 2-swap search

Crystal Grid: the unique back-circulant with |Aut|=648 (|Aut_total|=1296)
"""
import sys
import os
import time
import random
import json
import urllib.request
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from engine import CheckpointManager
from ortools.sat.python import cp_model

DASHBOARD_URL = "http://localhost:5000/api/worker_log"

# ══════════════════════════════════════════════════════════════
# Crystal Grid — hardcoded (dc_canonical: 001122334455667788...)
# ══════════════════════════════════════════════════════════════

CRYSTAL_GRID = (
    ((0,0), (1,1), (2,2), (3,3), (4,4), (5,5), (6,6), (7,7), (8,8)),
    ((3,6), (4,7), (5,8), (6,0), (7,1), (8,2), (0,3), (1,4), (2,5)),
    ((6,3), (7,4), (8,5), (0,6), (1,7), (2,8), (3,0), (4,1), (5,2)),
    ((1,2), (2,0), (0,1), (4,5), (5,3), (3,4), (7,8), (8,6), (6,7)),
    ((4,8), (5,6), (3,7), (7,2), (8,0), (6,1), (1,5), (2,3), (0,4)),
    ((7,5), (8,3), (6,4), (1,8), (2,6), (0,7), (4,2), (5,0), (3,1)),
    ((2,1), (0,2), (1,0), (5,4), (3,5), (4,3), (8,7), (6,8), (7,6)),
    ((5,7), (3,8), (4,6), (8,1), (6,2), (7,0), (2,4), (0,5), (1,3)),
    ((8,4), (6,5), (7,3), (2,7), (0,8), (1,6), (5,1), (3,2), (4,0)),
)


def send_log(label, msg, level="info"):
    """Send log to dashboard + print locally."""
    line = f"[{label}] {msg}"
    print(line, flush=True)
    try:
        data = json.dumps({"task": "task12", "msg": line, "level": level}).encode()
        req = urllib.request.Request(DASHBOARD_URL, data=data,
                                     headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=2)
    except Exception:
        pass


# ══════════════════════════════════════════════════════════════
# CP-SAT Solver — uniqueness check
# ══════════════════════════════════════════════════════════════

def count_solutions(clues, max_count=2, timeout=30):
    """Count solutions for a set of clues. Returns count (capped at max_count)."""
    class Counter(cp_model.CpSolverSolutionCallback):
        def __init__(self):
            super().__init__()
            self.count = 0
        def on_solution_callback(self):
            self.count += 1
            if self.count >= max_count:
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
            cells = [r for r in range(br*b, (br+1)*b)]
            cols = [c for c in range(bc*b, (bc+1)*b)]
            model.AddAllDifferent([digit[r][c] for r in cells for c in cols])
            model.AddAllDifferent([color[r][c] for r in cells for c in cols])

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


def has_unique_solution(clues, timeout=30):
    return count_solutions(clues, max_count=2, timeout=timeout) == 1


# ══════════════════════════════════════════════════════════════
# Phase 1 — Greedy removal (many shuffles)
# ══════════════════════════════════════════════════════════════

def greedy_removal(grid, label, shuffle_idx, timeout_per_check=30):
    """One greedy removal pass. Returns dict of clues."""
    clues = {(r, c): grid[r][c] for r in range(9) for c in range(9)}
    cells = list(clues.keys())
    random.shuffle(cells)
    removed = 0
    t0 = time.time()

    for cell in cells:
        trial = dict(clues)
        del trial[cell]
        if has_unique_solution(trial, timeout=timeout_per_check):
            clues = trial
            removed += 1

    dt = time.time() - t0
    n_clues = len(clues)
    send_log(label, f"  shuffle {shuffle_idx}: {n_clues} clues "
             f"({removed} removed) {dt:.0f}s")
    return clues


# ══════════════════════════════════════════════════════════════
# Phase 2 — Criticality check
# ══════════════════════════════════════════════════════════════

def verify_critical(clues, label, timeout=30):
    """
    Verify that the clue set is critical: removing ANY single clue
    makes the puzzle have >1 solution.
    Returns: (is_critical, removable_cells)
    """
    send_log(label, f"Phase 2: Criticality check on {len(clues)} clues...", "math")
    removable = []
    cells = sorted(clues.keys())
    for i, cell in enumerate(cells):
        trial = dict(clues)
        del trial[cell]
        nsol = count_solutions(trial, max_count=2, timeout=timeout)
        if nsol == 1:
            removable.append(cell)
            send_log(label, f"  ⚠ Clue at {cell} is removable! ({i+1}/{len(cells)})", "warning")
        if (i + 1) % 10 == 0:
            send_log(label, f"  criticality {i+1}/{len(cells)} checked "
                     f"({len(removable)} removable so far)")

    is_critical = len(removable) == 0
    if is_critical:
        send_log(label, f"  ✓ CRITICAL: all {len(clues)} clues are necessary", "success")
    else:
        send_log(label, f"  ✗ NOT critical: {len(removable)} clues removable", "warning")
    return is_critical, removable


# ══════════════════════════════════════════════════════════════
# Phase 3 — 2-swap improvement search
# ══════════════════════════════════════════════════════════════

def try_improve_by_swap(clues, grid, label, max_attempts=500, timeout=30):
    """
    Try to find a smaller clue set by:
    - Remove 2 clues, add 1 back from non-clue cells
    - Check if unique
    Returns improved clue set or None.
    """
    send_log(label, f"Phase 3: 2-swap search (max {max_attempts} attempts)...", "math")
    clue_cells = sorted(clues.keys())
    non_clue_cells = [(r, c) for r in range(9) for c in range(9)
                       if (r, c) not in clues]
    best = None
    for attempt in range(max_attempts):
        # Pick 2 random clues to remove
        remove = random.sample(clue_cells, 2)
        # Pick 1 random non-clue to add
        add_cell = random.choice(non_clue_cells)

        trial = dict(clues)
        for cell in remove:
            del trial[cell]
        trial[add_cell] = grid[add_cell[0]][add_cell[1]]

        # Now this has (n-1) clues. Check uniqueness.
        if has_unique_solution(trial, timeout=timeout):
            send_log(label, f"  ★ IMPROVEMENT: {len(trial)} clues! "
                     f"(removed {remove}, added {add_cell})", "success")
            best = trial
            break

        if (attempt + 1) % 50 == 0:
            send_log(label, f"  2-swap: {attempt+1}/{max_attempts} tried, no improvement")

    if best is None:
        send_log(label, f"  No improvement found in {max_attempts} attempts")
    return best


# ══════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════

def main():
    if len(sys.argv) < 3:
        print("Usage: python worker_task12.py <worker_id> <total_workers> [n_shuffles]")
        sys.exit(1)

    wid = int(sys.argv[1])
    n_workers = int(sys.argv[2])
    n_shuffles = int(sys.argv[3]) if len(sys.argv) > 3 else 100
    label = f"T12-W{wid:02d}"

    send_log(label, f"Starting — Crystal Grid minimum clues "
             f"(worker {wid}/{n_workers}, {n_shuffles} shuffles)", "success")

    # ── Resume checkpoint ──
    ckpt_name = f"task12_w{wid}"
    ckpt = CheckpointManager(ckpt_name)
    saved = ckpt.load()

    best_n = 81
    best_clues = None
    distribution = {}
    start_shuffle = 0
    all_results = []

    if saved and saved.get("status") == "done":
        send_log(label, f"Already DONE: min={saved.get('global_min')}. Skipping.", "success")
        return

    if saved and saved.get("status") == "running":
        best_n = saved.get("global_min", 81)
        best_clues = saved.get("best_clues")
        distribution = saved.get("distribution", {})
        start_shuffle = saved.get("n_shuffles_done", 0)
        all_results = saved.get("all_results", [])
        if best_clues:
            # Convert string keys back to tuples
            best_clues = {eval(k): tuple(v) for k, v in best_clues.items()}
        send_log(label, f"Resuming: {start_shuffle} shuffles done, "
                 f"best={best_n}", "warning")

    t_start = time.time()
    last_save = time.time()

    # ════════════════════════════════════════════════
    # Phase 1: Greedy removal
    # ════════════════════════════════════════════════
    send_log(label, f"Phase 1: {n_shuffles} greedy shuffles...", "math")

    for s in range(start_shuffle, n_shuffles):
        clues = greedy_removal(CRYSTAL_GRID, label, s + 1, timeout_per_check=30)
        n_clues = len(clues)

        dist_key = str(n_clues)
        distribution[dist_key] = distribution.get(dist_key, 0) + 1
        all_results.append(n_clues)

        if n_clues < best_n:
            best_n = n_clues
            best_clues = clues
            send_log(label, f"  ★★★ NEW BEST: {n_clues} clues ★★★ "
                     f"(shuffle {s+1}/{n_shuffles})", "success")
        else:
            elapsed = time.time() - t_start
            rate = (s - start_shuffle + 1) / max(0.1, elapsed)
            remaining = n_shuffles - s - 1
            eta = remaining / max(0.001, rate) / 60
            send_log(label, f"  shuffle {s+1}/{n_shuffles}: {n_clues} clues "
                     f"(best={best_n}) ETA {eta:.0f}min")

        # Checkpoint every 60s
        if time.time() - last_save > 60:
            _save_checkpoint(ckpt, wid, n_workers, s + 1, n_shuffles,
                             best_n, best_clues, distribution, all_results,
                             "running")
            last_save = time.time()

    _save_checkpoint(ckpt, wid, n_workers, n_shuffles, n_shuffles,
                     best_n, best_clues, distribution, all_results, "running")

    send_log(label, f"Phase 1 done: best = {best_n} clues "
             f"(from {n_shuffles} shuffles)", "success")
    send_log(label, f"  Distribution: {json.dumps(distribution, sort_keys=True)}", "math")

    # ════════════════════════════════════════════════
    # Phase 2: Criticality verification
    # ════════════════════════════════════════════════
    if best_clues:
        is_critical, removable = verify_critical(best_clues, label, timeout=30)

        if not is_critical and removable:
            # Remove the extra clues and re-check
            send_log(label, f"Removing {len(removable)} non-critical clues...", "warning")
            for cell in removable:
                del best_clues[cell]
            best_n = len(best_clues)
            send_log(label, f"Reduced to {best_n} clues", "success")

            # Re-verify criticality
            is_critical2, removable2 = verify_critical(best_clues, label, timeout=30)
            if removable2:
                for cell in removable2:
                    del best_clues[cell]
                best_n = len(best_clues)
                send_log(label, f"Further reduced to {best_n} clues", "success")

    # ════════════════════════════════════════════════
    # Phase 3: Try to beat by 2-swap
    # ════════════════════════════════════════════════
    if best_clues:
        improved = try_improve_by_swap(best_clues, CRYSTAL_GRID, label,
                                        max_attempts=1000, timeout=30)
        if improved:
            best_clues = improved
            best_n = len(improved)
            send_log(label, f"★ IMPROVED to {best_n} clues via 2-swap!", "success")

            # Verify criticality of improved set
            is_critical, removable = verify_critical(best_clues, label, timeout=30)
            if removable:
                for cell in removable:
                    del best_clues[cell]
                best_n = len(best_clues)

    # ════════════════════════════════════════════════
    # Final save
    # ════════════════════════════════════════════════
    elapsed = round(time.time() - t_start, 1)

    # Convert clues for JSON serialization
    clues_serializable = {}
    if best_clues:
        clues_serializable = {str(k): list(v) for k, v in best_clues.items()}

    result = {
        "status": "done",
        "worker_id": wid,
        "n_workers": n_workers,
        "n_shuffles": n_shuffles,
        "n_shuffles_done": n_shuffles,
        "global_min": best_n,
        "best_clues": clues_serializable,
        "distribution": distribution,
        "all_results": all_results,
        "elapsed": elapsed,
    }
    ckpt.save(result)

    send_log(label, f"═══ DONE: minimum = {best_n} clues "
             f"({n_shuffles} shuffles, {elapsed:.0f}s / {elapsed/60:.1f}min) ═══", "success")
    send_log(label, f"  Distribution: {json.dumps(distribution, sort_keys=True)}", "math")


def _save_checkpoint(ckpt, wid, n_workers, n_done, n_total,
                     best_n, best_clues, distribution, all_results, status):
    clues_ser = {}
    if best_clues:
        clues_ser = {str(k): list(v) for k, v in best_clues.items()}
    ckpt.save({
        "status": status,
        "worker_id": wid,
        "n_workers": n_workers,
        "n_shuffles": n_total,
        "n_shuffles_done": n_done,
        "global_min": best_n,
        "best_clues": clues_ser,
        "distribution": distribution,
        "all_results": all_results,
    })


if __name__ == "__main__":
    main()
