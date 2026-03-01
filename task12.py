#!/usr/bin/env python3
"""
Task 12 — Crystal Grid Minimum Clues (Proven)
Integrated dashboard workers, pauseable.

Phase 1: Greedy removal with many shuffles → upper bound
Phase 2: Criticality check — prove every clue is necessary
Phase 3: 2-swap improvement search
"""
import time
import random
import json
from pathlib import Path

from engine import CheckpointManager, LOG

try:
    from ortools.sat.python import cp_model
    CPSAT_AVAILABLE = True
except ImportError:
    CPSAT_AVAILABLE = False


# ══════════════════════════════════════════════════════════════
# Crystal Grid (dc_canonical: 001122334455667788...)
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


# ══════════════════════════════════════════════════════════════
# CP-SAT solver
# ══════════════════════════════════════════════════════════════

def _count_solutions(clues, max_count=2, timeout=30):
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
            cells_r = range(br*b, (br+1)*b)
            cells_c = range(bc*b, (bc+1)*b)
            model.AddAllDifferent([digit[r][c] for r in cells_r for c in cells_c])
            model.AddAllDifferent([color[r][c] for r in cells_r for c in cells_c])

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


def _has_unique(clues, timeout=30):
    return _count_solutions(clues, max_count=2, timeout=timeout) == 1


# ══════════════════════════════════════════════════════════════
# Greedy removal
# ══════════════════════════════════════════════════════════════

def _greedy_removal(grid, label, shuffle_idx, stop_event=None, timeout=30):
    clues = {(r, c): grid[r][c] for r in range(9) for c in range(9)}
    cells = list(clues.keys())
    random.shuffle(cells)
    removed = 0
    t0 = time.time()
    for cell in cells:
        if stop_event and stop_event.is_set():
            break
        trial = dict(clues)
        del trial[cell]
        if _has_unique(trial, timeout=timeout):
            clues = trial
            removed += 1
    dt = time.time() - t0
    LOG.add("task12", f"[{label}] shuffle {shuffle_idx}: {len(clues)} clues "
            f"({removed} removed) {dt:.0f}s")
    return clues


# ══════════════════════════════════════════════════════════════
# Criticality
# ══════════════════════════════════════════════════════════════

def _verify_critical(clues, label, stop_event=None, timeout=30):
    LOG.add("task12", f"[{label}] Criticality check on {len(clues)} clues...", level="math")
    removable = []
    cells = sorted(clues.keys())
    for i, cell in enumerate(cells):
        if stop_event and stop_event.is_set():
            break
        trial = dict(clues)
        del trial[cell]
        nsol = _count_solutions(trial, max_count=2, timeout=timeout)
        if nsol == 1:
            removable.append(cell)
            LOG.add("task12", f"[{label}] ⚠ Clue at {cell} removable! ({i+1}/{len(cells)})", level="warning")
        if (i + 1) % 10 == 0:
            LOG.add("task12", f"[{label}] criticality {i+1}/{len(cells)} "
                    f"({len(removable)} removable)")
    if not removable:
        LOG.add("task12", f"[{label}] ✓ CRITICAL: all {len(clues)} clues necessary", level="success")
    else:
        LOG.add("task12", f"[{label}] ✗ {len(removable)} removable", level="warning")
    return removable


# ══════════════════════════════════════════════════════════════
# 2-swap improvement
# ══════════════════════════════════════════════════════════════

def _try_2swap(clues, grid, label, max_attempts=500, stop_event=None, timeout=30):
    LOG.add("task12", f"[{label}] 2-swap search (max {max_attempts})...", level="math")
    clue_cells = sorted(clues.keys())
    non_clue = [(r, c) for r in range(9) for c in range(9) if (r, c) not in clues]
    for att in range(max_attempts):
        if stop_event and stop_event.is_set():
            break
        remove = random.sample(clue_cells, 2)
        add_cell = random.choice(non_clue)
        trial = dict(clues)
        for cell in remove:
            del trial[cell]
        trial[add_cell] = grid[add_cell[0]][add_cell[1]]
        if _has_unique(trial, timeout=timeout):
            LOG.add("task12", f"[{label}] ★ IMPROVED to {len(trial)} clues!", level="success")
            return trial
        if (att + 1) % 100 == 0:
            LOG.add("task12", f"[{label}] 2-swap: {att+1}/{max_attempts}, no improvement")
    return None


# ══════════════════════════════════════════════════════════════
# Main worker (dashboard-integrated, pauseable)
# ══════════════════════════════════════════════════════════════

def task12_worker(worker_id, n_shuffles=50,
                  progress_callback=None, stop_event=None):
    label = f"W{worker_id}"

    if not CPSAT_AVAILABLE:
        msg = "ortools non disponible"
        LOG.add("task12", f"[{label}] {msg}", level="error")
        return {"status": "error", "message": msg}

    # Resume
    ckpt_name = f"task12_w{worker_id}"
    ckpt = CheckpointManager(ckpt_name)
    saved = ckpt.load()

    best_n = 81
    best_clues = None
    distribution = {}
    start_shuffle = 0

    if saved and saved.get("status") == "done":
        LOG.add("task12", f"[{label}] Already done: min={saved.get('global_min')}", level="success")
        return {"status": "done", "global_min": saved.get("global_min", 81),
                "n_shuffles_done": saved.get("n_shuffles_done", 0),
                "distribution": saved.get("distribution", {})}

    if saved and saved.get("status") in ("running", "paused"):
        best_n = saved.get("global_min", 81)
        best_clues = saved.get("best_clues")
        distribution = saved.get("distribution", {})
        start_shuffle = saved.get("n_shuffles_done", 0)
        if best_clues:
            best_clues = {eval(k): tuple(v) for k, v in best_clues.items()}
        LOG.add("task12", f"[{label}] Resuming: {start_shuffle} done, best={best_n}", level="warning")

    LOG.add("task12", f"[{label}] START — {n_shuffles} shuffles, resume={start_shuffle}", level="success")

    t0 = time.time()
    last_save = time.time()
    stopped = False

    # Phase 1: Greedy
    for s in range(start_shuffle, n_shuffles):
        if stop_event and stop_event.is_set():
            stopped = True
            break

        clues = _greedy_removal(CRYSTAL_GRID, label, s + 1, stop_event=stop_event)
        n_clues = len(clues)
        dist_key = str(n_clues)
        distribution[dist_key] = distribution.get(dist_key, 0) + 1

        if n_clues < best_n:
            best_n = n_clues
            best_clues = clues
            LOG.add("task12", f"[{label}] ★★★ NEW BEST: {n_clues} clues (shuffle {s+1})", level="success")

        elapsed = time.time() - t0
        done = s - start_shuffle + 1
        rate = done / max(0.1, elapsed)
        remaining = n_shuffles - s - 1
        eta = remaining / max(0.001, rate) / 60
        pct = min(98, max(2, int(98 * (s + 1) / n_shuffles)))

        if progress_callback:
            progress_callback({
                "phase": "greedy", "percent": pct,
                "message": f"Shuffle {s+1}/{n_shuffles}: {n_clues} (best={best_n}) ETA {eta:.0f}min",
                "global_min": best_n,
            })

        if time.time() - last_save > 60:
            _save(ckpt, worker_id, s + 1, n_shuffles, best_n, best_clues, distribution, "running")
            last_save = time.time()

    if not stopped and best_clues:
        # Phase 2: Criticality
        if progress_callback:
            progress_callback({"phase": "critical", "percent": 98,
                               "message": f"Criticality check ({best_n} clues)..."})
        removable = _verify_critical(best_clues, label, stop_event=stop_event)
        if removable and not (stop_event and stop_event.is_set()):
            for cell in removable:
                del best_clues[cell]
            best_n = len(best_clues)
            LOG.add("task12", f"[{label}] Reduced to {best_n} after criticality", level="success")
            # Re-check
            removable2 = _verify_critical(best_clues, label, stop_event=stop_event)
            if removable2 and not (stop_event and stop_event.is_set()):
                for cell in removable2:
                    del best_clues[cell]
                best_n = len(best_clues)

        # Phase 3: 2-swap
        if not (stop_event and stop_event.is_set()):
            if progress_callback:
                progress_callback({"phase": "2swap", "percent": 99,
                                   "message": f"2-swap search ({best_n} clues)..."})
            improved = _try_2swap(best_clues, CRYSTAL_GRID, label,
                                  max_attempts=1000, stop_event=stop_event)
            if improved:
                best_clues = improved
                best_n = len(improved)
                removable = _verify_critical(best_clues, label, stop_event=stop_event)
                if removable and not (stop_event and stop_event.is_set()):
                    for cell in removable:
                        del best_clues[cell]
                    best_n = len(best_clues)

    stopped = stopped or (stop_event and stop_event.is_set())
    status = "paused" if stopped else "done"
    n_done = min(n_shuffles, start_shuffle + (0 if stopped else n_shuffles - start_shuffle))
    # Try to get accurate count
    total_done = sum(distribution.values())

    _save(ckpt, worker_id, total_done, n_shuffles, best_n, best_clues, distribution, status)

    elapsed = round(time.time() - t0, 1)
    LOG.add("task12", f"[{label}] {'STOPPED' if stopped else 'DONE'}: "
            f"min={best_n}, {total_done} shuffles, {elapsed:.0f}s", level="success")

    if progress_callback:
        progress_callback({"phase": "done", "percent": 100,
                           "message": f"{'Stopped' if stopped else 'Done'}: min={best_n} clues"})

    return {"status": status, "global_min": best_n,
            "n_shuffles_done": total_done, "n_shuffles": n_shuffles,
            "distribution": distribution, "elapsed": elapsed}


def _save(ckpt, wid, n_done, n_total, best_n, best_clues, distribution, status):
    clues_ser = {}
    if best_clues:
        clues_ser = {str(k): list(v) for k, v in best_clues.items()}
    ckpt.save({
        "status": status, "worker_id": wid,
        "n_shuffles": n_total, "n_shuffles_done": n_done,
        "global_min": best_n, "best_clues": clues_ser,
        "distribution": distribution,
    })


# ══════════════════════════════════════════════════════════════
# Summary (aggregate all workers)
# ══════════════════════════════════════════════════════════════

def task12_get_summary():
    global_min = 81
    n_done = 0
    total_shuffles = 0
    merged_dist = {}
    for wid in range(100):
        ckpt = CheckpointManager(f"task12_w{wid}")
        saved = ckpt.load()
        if not saved:
            continue
        wmin = saved.get("global_min", 81)
        if wmin < global_min:
            global_min = wmin
        total_shuffles += saved.get("n_shuffles_done", 0)
        if saved.get("status") == "done":
            n_done += 1
        for k, v in saved.get("distribution", {}).items():
            merged_dist[k] = merged_dist.get(k, 0) + v
    return {
        "global_min": global_min, "n_workers_done": n_done,
        "total_shuffles": total_shuffles, "distribution": merged_dist,
    }
