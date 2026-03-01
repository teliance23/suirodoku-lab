"""
Suirodoku Lab -- Computation Engine v10 (Unified)
========================================================
Full engine: Tasks 1-9 + Task 10 Hunter (integrated). No more separate files.
- Task 3: CP-SAT model with double symmetry breaking (digit + color first row)
- Task 4: canonicalization tests 3.35M transforms instead of 6.7M (2x speedup)
- Task 7: uniqueness checker (CP-SAT, no rotation constraint)
- Task 9: σ↔τ swap — digit/color role exchange orbit merging
"""

import json
import os
import time
import hashlib
import itertools
import math
import random
import threading
import multiprocessing as mp
from datetime import datetime
from pathlib import Path
from collections import deque
import ctypes

# ─── C acceleration for canonicalization ───
_CANON_LIB = None
_CANON_LIB_CHECKED = False

def _load_canon_lib():
    """Load the compiled canon.so/canon.dll C library."""
    global _CANON_LIB, _CANON_LIB_CHECKED
    if _CANON_LIB is not None:
        return _CANON_LIB
    if _CANON_LIB_CHECKED:
        return None  # Already tried and failed
    _CANON_LIB_CHECKED = True
    import platform
    ext = '.dll' if platform.system() == 'Windows' else '.so'
    search_paths = []
    try:
        search_paths.append(Path(__file__).parent)
    except NameError:
        pass
    search_paths.append(Path('.'))
    for base in search_paths:
        p = base / f'canon{ext}'
        if p.exists():
            try:
                _CANON_LIB = ctypes.CDLL(str(p))
                _CANON_LIB.canonicalize_and_stab.argtypes = [
                    ctypes.POINTER(ctypes.c_int), ctypes.POINTER(ctypes.c_int),
                    ctypes.POINTER(ctypes.c_int), ctypes.POINTER(ctypes.c_int),
                ]
                _CANON_LIB.canonicalize_and_stab.restype = ctypes.c_int
                _CANON_LIB.canonicalize_only.argtypes = [
                    ctypes.POINTER(ctypes.c_int), ctypes.POINTER(ctypes.c_int),
                    ctypes.POINTER(ctypes.c_int),
                ]
                _CANON_LIB.canonicalize_only.restype = ctypes.c_int
                return _CANON_LIB
            except OSError as e:
                print(f"Warning: found {p} but failed to load: {e}")
    return None

_IntArr81 = ctypes.c_int * 81

def canon_lib_available():
    """Check if the C canonicalization library is available."""
    return _load_canon_lib() is not None

def fast_canonicalize_and_stab(grid):
    """Canonicalize grid + compute stabilizer via C library.
    grid: tuple of tuples ((d,k), ...) in 0-indexed.
    Returns: (canon_hash, canon_flat, stab_size)
    REQUIRES canon.dll/.so — raises RuntimeError if not found.
    """
    lib = _load_canon_lib()
    if lib is None:
        raise RuntimeError(
            "canon.dll/.so not found! Compile it first:\n"
            "  Windows: gcc -O3 -shared -o canon.dll canon.c\n"
            "  Linux:   gcc -O3 -shared -fPIC -o canon.so canon.c\n"
            "Place the file next to engine.py."
        )

    gd = _IntArr81(*(grid[r][c][0] for r in range(9) for c in range(9)))
    gk = _IntArr81(*(grid[r][c][1] for r in range(9) for c in range(9)))
    cout = _IntArr81()
    sout = ctypes.c_int(0)
    lib.canonicalize_and_stab(gd, gk, cout, ctypes.byref(sout))
    canon_flat = tuple(cout)
    h = hashlib.md5(str(canon_flat).encode()).hexdigest()
    return h, canon_flat, sout.value

def fast_canonicalize(grid):
    """Canonicalize grid via C library (no stabilizer).
    Returns: (canon_hash, canon_flat)
    REQUIRES canon.dll/.so — raises RuntimeError if not found.
    """
    lib = _load_canon_lib()
    if lib is None:
        raise RuntimeError(
            "canon.dll/.so not found! Compile it first:\n"
            "  Windows: gcc -O3 -shared -o canon.dll canon.c\n"
            "  Linux:   gcc -O3 -shared -fPIC -o canon.so canon.c\n"
            "Place the file next to engine.py."
        )

    gd = _IntArr81(*(grid[r][c][0] for r in range(9) for c in range(9)))
    gk = _IntArr81(*(grid[r][c][1] for r in range(9) for c in range(9)))
    cout = _IntArr81()
    lib.canonicalize_only(gd, gk, cout)
    canon_flat = tuple(cout)
    h = hashlib.md5(str(canon_flat).encode()).hexdigest()
    return h, canon_flat

CHECKPOINT_DIR = Path(__file__).parent / "checkpoints"
CHECKPOINT_DIR.mkdir(exist_ok=True)

EXPORT_DIR = Path(__file__).parent / "exports"
EXPORT_DIR.mkdir(exist_ok=True)


# =============================================================
# Live Log System
# =============================================================

class LiveLog:
    def __init__(self, max_entries=500):
        self.entries = deque(maxlen=max_entries)

    def add(self, task_id, message, level="info", data=None):
        self.entries.append({
            "t": datetime.now().strftime("%H:%M:%S"),
            "task": task_id,
            "msg": message,
            "level": level,
            "data": data,
        })

    def get_recent(self, n=100):
        return list(self.entries)[-n:]

LOG = LiveLog()

# Dynamic target for Task 3 — can be changed at runtime via API
TASK3_TARGET = 5000

def set_task3_target(n):
    global TASK3_TARGET
    TASK3_TARGET = max(1, int(n))
    return TASK3_TARGET

def get_task3_target():
    return TASK3_TARGET


# =============================================================
# Checkpoint Manager
# =============================================================

class CheckpointManager:
    def __init__(self, task_id):
        self.task_id = task_id
        self.filepath = CHECKPOINT_DIR / f"{task_id}.json"

    def save(self, data):
        data["_meta"] = {
            "task_id": self.task_id,
            "saved_at": datetime.now().isoformat(),
        }
        tmp = self.filepath.with_suffix(".tmp")
        with open(tmp, "w") as f:
            json.dump(data, f, default=str)
            f.flush()
            os.fsync(f.fileno())
        # Retry rename on Windows PermissionError (file locking by other processes)
        for attempt in range(30):
            try:
                tmp.replace(self.filepath)
                return
            except (PermissionError, OSError):
                import time as _time
                _time.sleep(1.0)
        # Last resort
        try:
            tmp.replace(self.filepath)
        except Exception as e:
            # If rename still fails, try copy+delete as fallback
            import shutil
            try:
                shutil.copy2(str(tmp), str(self.filepath))
                tmp.unlink(missing_ok=True)
            except Exception:
                pass

    def load(self):
        for attempt in range(5):
            try:
                if self.filepath.exists():
                    with open(self.filepath) as f:
                        return json.load(f)
                return None
            except (PermissionError, json.JSONDecodeError):
                import time as _time
                _time.sleep(0.3)
        return None

    def load_sample(self):
        """Load sample data. Tries separate sample file first, then inline, then sub-partitions."""
        main = self.load()
        if not main or main.get("status") != "done":
            return None

        # Try separate sample file
        sample_file = main.get("sample_file")
        if sample_file:
            sample_path = self.filepath.parent / sample_file
            try:
                with open(sample_path) as f:
                    return json.load(f)
            except Exception:
                pass

        # Try inline sample
        if "sample" in main and main["sample"]:
            return main["sample"]

        # For merged checkpoints: load from sub-partitions
        if "task10g" in self.task_id and "order3_mates" in self.task_id:
            all_samples = []
            for pid in range(1, 9):
                for sub in ('a', 'b'):
                    sp_name = f"task10g_p{pid}{sub}_order3"
                    sp_ckpt = CheckpointManager(sp_name)
                    sp_sample = sp_ckpt._load_sample_direct()
                    if sp_sample:
                        all_samples.extend(sp_sample)
            return all_samples if all_samples else None

        if "task10i" in self.task_id and "unbiased_mates" in self.task_id:
            all_samples = []
            for pid in range(1, 9):
                for sub in ('a', 'b'):
                    sp_name = f"task10i_p{pid}{sub}_unbiased"
                    sp_ckpt = CheckpointManager(sp_name)
                    sp_sample = sp_ckpt._load_sample_direct()
                    if sp_sample:
                        all_samples.extend(sp_sample)
            return all_samples if all_samples else None

        return None

    def _load_sample_direct(self):
        """Load sample from separate file or inline (no recursion)."""
        main = self.load()
        if not main or main.get("status") != "done":
            return None
        sample_file = main.get("sample_file")
        if sample_file:
            sample_path = self.filepath.parent / sample_file
            try:
                with open(sample_path) as f:
                    return json.load(f)
            except Exception:
                pass
        if "sample" in main and main["sample"]:
            return main["sample"]
        return None

    def clear(self):
        if self.filepath.exists():
            self.filepath.unlink()


# =============================================================
# Reference Grid G0 (known valid Suirodoku)
# =============================================================

COLOR_MAP = {'R': 0, 'G': 1, 'B': 2, 'M': 3, 'O': 4, 'P': 5, 'T': 6, 'V': 7, 'J': 8}
COLOR_NAMES = ['R', 'G', 'B', 'M', 'O', 'P', 'T', 'V', 'J']

MODEL_GRID_RAW = [
    [(3,'V'),(5,'B'),(2,'G'),(6,'M'),(4,'R'),(7,'P'),(1,'T'),(9,'O'),(8,'J')],
    [(1,'P'),(7,'J'),(9,'R'),(2,'O'),(3,'B'),(8,'T'),(4,'V'),(6,'G'),(5,'M')],
    [(6,'O'),(8,'M'),(4,'T'),(5,'V'),(9,'J'),(1,'G'),(2,'P'),(3,'R'),(7,'B')],
    [(2,'J'),(9,'P'),(8,'V'),(3,'T'),(7,'M'),(6,'R'),(5,'O'),(1,'B'),(4,'G')],
    [(4,'B'),(6,'T'),(7,'O'),(1,'J'),(5,'G'),(9,'V'),(3,'M'),(8,'P'),(2,'R')],
    [(5,'R'),(3,'G'),(1,'M'),(4,'P'),(8,'O'),(2,'B'),(6,'J'),(7,'V'),(9,'T')],
    [(8,'G'),(2,'V'),(3,'J'),(9,'B'),(6,'P'),(4,'M'),(7,'R'),(5,'T'),(1,'O')],
    [(7,'T'),(4,'O'),(6,'B'),(8,'R'),(1,'V'),(5,'J'),(9,'G'),(2,'M'),(3,'P')],
    [(9,'M'),(1,'R'),(5,'P'),(7,'G'),(2,'T'),(3,'O'),(8,'B'),(4,'J'),(6,'V')],
]

def parse_model_grid():
    grid = []
    for row in MODEL_GRID_RAW:
        r = []
        for num, col_letter in row:
            r.append((num - 1, COLOR_MAP[col_letter]))
        grid.append(tuple(r))
    return tuple(grid)

MODEL_GRID = parse_model_grid()


def grid_hash(grid):
    return hashlib.md5(str(grid).encode()).hexdigest()

def grid_to_display(grid):
    return [[{"d": d + 1, "c": COLOR_NAMES[c]} for d, c in row] for row in grid]

def apply_row_perm(grid, perm):
    return tuple(grid[perm[i]] for i in range(9))

def apply_col_perm(grid, perm):
    return tuple(tuple(row[perm[j]] for j in range(9)) for row in grid)

def rotate_90(grid):
    return tuple(tuple(grid[8 - j][i] for j in range(9)) for i in range(9))

def check_is_relabeling(grid_a, grid_b):
    digit_map = {}
    color_map = {}
    for r in range(9):
        for c in range(9):
            da, ca = grid_a[r][c]
            db, cb = grid_b[r][c]
            if da in digit_map:
                if digit_map[da] != db: return False, None, None
            else: digit_map[da] = db
            if ca in color_map:
                if color_map[ca] != cb: return False, None, None
            else: color_map[ca] = cb
    if len(set(digit_map.values())) != len(digit_map): return False, None, None
    if len(set(color_map.values())) != len(color_map): return False, None, None
    return True, digit_map, color_map

def verify_suirodoku(grid):
    n = 9
    checks = {}
    for label, extract in [
        ("digit_rows", lambda: [set(grid[r][c][0] for c in range(n)) for r in range(n)]),
        ("digit_cols", lambda: [set(grid[r][c][0] for r in range(n)) for c in range(n)]),
        ("digit_blocks", lambda: [set(grid[r][c][0] for r in range(br*3,(br+1)*3) for c in range(bc*3,(bc+1)*3)) for br in range(3) for bc in range(3)]),
        ("color_rows", lambda: [set(grid[r][c][1] for c in range(n)) for r in range(n)]),
        ("color_cols", lambda: [set(grid[r][c][1] for r in range(n)) for c in range(n)]),
        ("color_blocks", lambda: [set(grid[r][c][1] for r in range(br*3,(br+1)*3) for c in range(bc*3,(bc+1)*3)) for br in range(3) for bc in range(3)]),
    ]:
        checks[label] = all(len(s) == n for s in extract())
    pairs = [(grid[r][c][0], grid[r][c][1]) for r in range(n) for c in range(n)]
    checks["pair_uniqueness"] = len(set(pairs)) == n * n
    checks["all_valid"] = all(checks.values())
    return checks


def swap_grid(grid):
    """Apply the universal d↔k swap: each cell (d,k) becomes (k,d).
    Returns a new grid (tuple of tuples)."""
    return tuple(
        tuple((grid[r][c][1], grid[r][c][0]) for c in range(9))
        for r in range(9)
    )


# =============================================================
# Rot180 Constraint Helper
# =============================================================
# Imposes central symmetry ρ₁₈₀ on both digit and color layers.
# This is a BIAS — most orbits (stab=1) do NOT have rot180 symmetry.

def add_rot180_constraints(model, digit, color, n=9):
    """Add central symmetry constraints to a CP-SAT model (rot180 bias).
    
    Creates σ (digit relabeling) and τ (color relabeling) as permutation
    variables, then constrains the lower half of the grid to mirror the
    upper half through (4,4) under these relabelings.
    
    Args:
        model: cp_model.CpModel
        digit: 9x9 list of IntVar (digit variables)
        color: 9x9 list of IntVar (color variables)
    
    Returns:
        (sigma, tau) — the permutation variable lists
    """
    # σ and τ: permutations of {0,...,8}
    sigma = [model.NewIntVar(0, n-1, f'sigma_{i}') for i in range(n)]
    tau = [model.NewIntVar(0, n-1, f'tau_{i}') for i in range(n)]
    model.AddAllDifferent(sigma)
    model.AddAllDifferent(tau)

    # Upper half: rows 0-3 + cells (4,0)..(4,3)
    # For each cell (r,c) in upper half (excluding center):
    #   digit[8-r][8-c] = sigma[digit[r][c]]
    #   color[8-r][8-c] = tau[color[r][c]]
    for r in range(n):
        for c in range(n):
            # Only process each pair once: upper half
            mr, mc = 8 - r, 8 - c
            if (r, c) >= (mr, mc):
                continue  # skip lower half and center
            # Element constraint: sigma[digit[r][c]] == digit[mr][mc]
            model.AddElement(digit[r][c], sigma, digit[mr][mc])
            model.AddElement(color[r][c], tau, color[mr][mc])

    # Center cell (4,4): must be fixed point of both σ and τ
    # σ(d₄₄) = d₄₄ and τ(k₄₄) = k₄₄
    model.AddElement(digit[4][4], sigma, digit[4][4])
    model.AddElement(color[4][4], tau, color[4][4])

    return sigma, tau


def add_rot180_color_only(model, color, digit_grid, n=9):
    """Add rot180 constraint when the digit layer is FIXED (not CP-SAT vars).

    Checks if the digit grid has rot180 symmetry. If yes, computes σ from it
    and adds τ (color permutation) constraints only.

    Args:
        model: cp_model.CpModel
        color: n×n list of IntVar (color variables)
        digit_grid: n×n list of ints (1-9, fixed)

    Returns:
        tau (list of IntVar) if digit grid has rot180, else None
    """
    # Check if digit grid has rot180: find σ such that d[8-r][8-c] = σ(d[r][c])
    sigma_map = {}
    for r in range(n):
        for c in range(n):
            d_rc = digit_grid[r][c] - 1      # 0-indexed
            d_mr = digit_grid[8-r][8-c] - 1
            if d_rc in sigma_map:
                if sigma_map[d_rc] != d_mr:
                    return None  # digit grid doesn't have rot180
            sigma_map[d_rc] = d_mr

    if len(sigma_map) != n or len(set(sigma_map.values())) != n:
        return None

    # Digit grid has rot180. Add τ for colors.
    tau = [model.NewIntVar(0, n-1, f'tau_{i}') for i in range(n)]
    model.AddAllDifferent(tau)

    for r in range(n):
        for c in range(n):
            mr, mc = 8 - r, 8 - c
            if (r, c) >= (mr, mc):
                continue
            # Both directions: tau[k(r,c)] = k(mr,mc) AND tau[k(mr,mc)] = k(r,c)
            model.AddElement(color[r][c], tau, color[mr][mc])
            model.AddElement(color[mr][mc], tau, color[r][c])

    # Center cell: fixed point
    model.AddElement(color[4][4], tau, color[4][4])

    return tau


def add_C3_involution_color_only(model, color, digit_grid, n=9):
    """C3 class: order-2 involution (NOT r180), 18 elements, 9 fixed cells.
    Cell mapping: (r,c) -> (RP[r], CP[c])."""
    RP = [0, 2, 1, 3, 5, 4, 8, 7, 6]
    CP = [0, 1, 2, 6, 7, 8, 3, 4, 5]
    sigma_map = {}
    for r in range(n):
        for c in range(n):
            d_rc = digit_grid[r][c] - 1
            d_mapped = digit_grid[RP[r]][CP[c]] - 1
            if d_rc in sigma_map:
                if sigma_map[d_rc] != d_mapped:
                    return None
            sigma_map[d_rc] = d_mapped
    if len(sigma_map) != n or len(set(sigma_map.values())) != n:
        return None
    tau = [model.NewIntVar(0, n-1, f'tau_c3_{i}') for i in range(n)]
    model.AddAllDifferent(tau)
    done = set()
    for r in range(n):
        for c in range(n):
            r2, c2 = RP[r], CP[c]
            key = (min((r,c),(r2,c2)), max((r,c),(r2,c2)))
            if key in done:
                continue
            done.add(key)
            if (r, c) == (r2, c2):
                model.AddElement(color[r][c], tau, color[r][c])
            else:
                # Both directions for involution
                model.AddElement(color[r][c], tau, color[r2][c2])
                model.AddElement(color[r2][c2], tau, color[r][c])
    return tau


def add_C4_transpose_color_only(model, color, digit_grid, n=9):
    """C4 class: order-2 transpose involution, 18 elements, 9 fixed cells.
    Cell mapping: (r,c) -> (RP[c], CP[r]) — transpose!"""
    RP = [0, 1, 2, 3, 5, 4, 7, 8, 6]
    CP = [0, 1, 2, 3, 5, 4, 8, 6, 7]
    sigma_map = {}
    for r in range(n):
        for c in range(n):
            d_rc = digit_grid[r][c] - 1
            d_mapped = digit_grid[RP[c]][CP[r]] - 1
            if d_rc in sigma_map:
                if sigma_map[d_rc] != d_mapped:
                    return None
            sigma_map[d_rc] = d_mapped
    if len(sigma_map) != n or len(set(sigma_map.values())) != n:
        return None
    tau = [model.NewIntVar(0, n-1, f'tau_c4_{i}') for i in range(n)]
    model.AddAllDifferent(tau)
    done = set()
    for r in range(n):
        for c in range(n):
            r2, c2 = RP[c], CP[r]
            key = (min((r,c),(r2,c2)), max((r,c),(r2,c2)))
            if key in done:
                continue
            done.add(key)
            if (r, c) == (r2, c2):
                model.AddElement(color[r][c], tau, color[r][c])
            else:
                # Both directions for involution
                model.AddElement(color[r][c], tau, color[r2][c2])
                model.AddElement(color[r2][c2], tau, color[r][c])
    return tau


def add_order3_cycle_color_only(model, color, digit_grid, n=9):
    """Order-3 cycle: cols 0->1->2->0 within each stack, rows unchanged.
    Cell mapping: (r,c) -> (r, CP[c]) where CP=[1,2,0,4,5,3,7,8,6]."""
    CP = [1, 2, 0, 4, 5, 3, 7, 8, 6]
    sigma_map = {}
    for r in range(n):
        for c in range(n):
            d_rc = digit_grid[r][c] - 1
            d_mapped = digit_grid[r][CP[c]] - 1
            if d_rc in sigma_map:
                if sigma_map[d_rc] != d_mapped:
                    return None
            sigma_map[d_rc] = d_mapped
    if len(sigma_map) != n or len(set(sigma_map.values())) != n:
        return None
    tau = [model.NewIntVar(0, n-1, f'tau_o3_{i}') for i in range(n)]
    model.AddAllDifferent(tau)
    done = set()
    for r in range(n):
        for c in range(n):
            c1 = CP[c]
            c2 = CP[c1]
            orbit = tuple(sorted([(r,c), (r,c1), (r,c2)]))
            if orbit in done:
                continue
            done.add(orbit)
            model.AddElement(color[r][c], tau, color[r][c1])
            model.AddElement(color[r][c1], tau, color[r][c2])
            model.AddElement(color[r][c2], tau, color[r][c])   # close the 3-cycle
    return tau


def add_s162_involution_color_only(model, color, digit_grid, n=9):
    """Stab-162 order-2 involution: swap bands 1↔2, swap cols 1↔2 within each stack.
    Cell mapping: (r,c) -> (RP[r], CP[c]).
    9 fixed cells (band 0, col 0 of each stack), 36 two-cycles.
    One of the 9 involutions in Aut(stab162)."""
    RP = [0, 1, 2, 6, 7, 8, 3, 4, 5]
    CP = [0, 2, 1, 3, 5, 4, 6, 8, 7]
    sigma_map = {}
    for r in range(n):
        for c in range(n):
            d_rc = digit_grid[r][c] - 1
            d_mapped = digit_grid[RP[r]][CP[c]] - 1
            if d_rc in sigma_map:
                if sigma_map[d_rc] != d_mapped:
                    return None
            sigma_map[d_rc] = d_mapped
    if len(sigma_map) != n or len(set(sigma_map.values())) != n:
        return None
    tau = [model.NewIntVar(0, n-1, f'tau_s162inv_{i}') for i in range(n)]
    model.AddAllDifferent(tau)
    done = set()
    for r in range(n):
        for c in range(n):
            r2, c2 = RP[r], CP[c]
            key = (min((r,c),(r2,c2)), max((r,c),(r2,c2)))
            if key in done:
                continue
            done.add(key)
            if (r, c) == (r2, c2):
                model.AddElement(color[r][c], tau, color[r][c])
            else:
                # Both directions: tau[k(r,c)] = k(r2,c2) AND tau[k(r2,c2)] = k(r,c)
                model.AddElement(color[r][c], tau, color[r2][c2])
                model.AddElement(color[r2][c2], tau, color[r][c])
    return tau


def add_s162_stack3_color_only(model, color, digit_grid, n=9):
    """Stab-162 order-3 cycle: cycle stacks 0→1→2→0, rows unchanged.
    Cell mapping: (r,c) -> (r, CP[c]) where CP=[3,4,5,6,7,8,0,1,2].
    0 fixed cells, 27 three-cycles. One of the 80 order-3 elements in Aut(stab162)."""
    CP = [3, 4, 5, 6, 7, 8, 0, 1, 2]
    sigma_map = {}
    for r in range(n):
        for c in range(n):
            d_rc = digit_grid[r][c] - 1
            d_mapped = digit_grid[r][CP[c]] - 1
            if d_rc in sigma_map:
                if sigma_map[d_rc] != d_mapped:
                    return None
            sigma_map[d_rc] = d_mapped
    if len(sigma_map) != n or len(set(sigma_map.values())) != n:
        return None
    tau = [model.NewIntVar(0, n-1, f'tau_s162o3_{i}') for i in range(n)]
    model.AddAllDifferent(tau)
    done = set()
    for r in range(n):
        for c in range(n):
            c1 = CP[c]
            c2 = CP[c1]
            orbit = tuple(sorted([(r,c), (r,c1), (r,c2)]))
            if orbit in done:
                continue
            done.add(orbit)
            model.AddElement(color[r][c], tau, color[r][c1])
            model.AddElement(color[r][c1], tau, color[r][c2])
            model.AddElement(color[r][c2], tau, color[r][c])   # close the 3-cycle
    return tau


# =============================================================
# Structural Symmetry
# =============================================================

def enumerate_structural_transforms():
    """Enumerate all 3,359,232 distinct structural transforms.
    rot ∈ {0, 1} only: rot180 (rot=2) is already expressible as
    row_reverse ∘ col_reverse (both in S3≀S3), so rot=2 and rot=3
    duplicate rot=0 and rot=1 respectively."""
    s3 = list(itertools.permutations([0, 1, 2]))
    for band_perm in s3:
        for stack_perm in s3:
            for rb0 in s3:
                for rb1 in s3:
                    for rb2 in s3:
                        for cs0 in s3:
                            for cs1 in s3:
                                for cs2 in s3:
                                    for rot in range(2):  # 0 and 1 only
                                        row_bands = [rb0, rb1, rb2]
                                        row_perm = []
                                        for bi in range(3):
                                            src_band = band_perm[bi]
                                            for ri in range(3):
                                                row_perm.append(src_band * 3 + row_bands[src_band][ri])
                                        col_stacks = [cs0, cs1, cs2]
                                        col_perm = []
                                        for si in range(3):
                                            src_stack = stack_perm[si]
                                            for ci in range(3):
                                                col_perm.append(src_stack * 3 + col_stacks[src_stack][ci])
                                        yield (tuple(row_perm), tuple(col_perm), rot)

def apply_structural_transform(grid, row_perm, col_perm, rotations):
    result = apply_row_perm(grid, row_perm)
    result = apply_col_perm(result, col_perm)
    for _ in range(rotations):
        result = rotate_90(result)
    return result

TOTAL_STRUCTURAL = (6**6) * 6 * 6 * 2  # 3,359,232 (rot 0,1 only — rot180 is in row/col perms)




# =============================================================
# Task 1: Exact 4x4 Count
# =============================================================

def task1_exact_4x4(progress_callback=None):
    from ortools.sat.python import cp_model

    ckpt = CheckpointManager("task1_exact_4x4")
    saved = ckpt.load()
    if saved and saved.get("status") == "done":
        return saved

    LOG.add("task1", "=== EXACT COUNT 4x4 ===", level="success")
    LOG.add("task1", "Building CSP model...")
    LOG.add("task1", "Variables: 16 digit + 16 color + 16 pair = 48 vars", level="math")
    LOG.add("task1", "Pair encoding: pair[r][c] = digit[r][c] * 4 + color[r][c]", level="math")
    LOG.add("task1", "AllDifferent constraints:", level="math")
    LOG.add("task1", "  Digits: 4 rows + 4 cols + 4 blocks = 12", level="math")
    LOG.add("task1", "  Colors: 4 rows + 4 cols + 4 blocks = 12", level="math")
    LOG.add("task1", "  Global pairs: 1 AllDifferent on 16 cells", level="math")
    LOG.add("task1", "  Total: 25 AllDifferent + 16 linear encoding", level="math")
    LOG.add("task1", "Solver: OR-Tools CP-SAT, 1 thread, enumerate_all_solutions=True")

    class Counter(cp_model.CpSolverSolutionCallback):
        def __init__(self):
            super().__init__()
            self.count = 0
            self.last_report = time.time()
        def on_solution_callback(self):
            self.count += 1
            now = time.time()
            if now - self.last_report > 0.3:
                self.last_report = now
                if progress_callback: progress_callback(self.count)

    n, b = 4, 2
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

    solver = cp_model.CpSolver()
    solver.parameters.enumerate_all_solutions = True
    solver.parameters.num_workers = 1

    counter = Counter()
    start = time.time()
    LOG.add("task1", "Enumeration started...")
    solver.Solve(model, counter)
    elapsed = time.time() - start

    LOG.add("task1", f"RESULT: |S4| = {counter.count:,} Suirodoku 4x4 grids", level="success")
    LOG.add("task1", f"Factorization: {counter.count} = 2^8 * 3^2", level="math")
    LOG.add("task1", f"Reference: 288 Sudoku 4x4 grids (Seta, 2012)", level="math")
    LOG.add("task1", f"Ratio: {counter.count}/288 = {counter.count/288:.1f}x", level="math")
    LOG.add("task1", f"Time: {elapsed:.2f}s")

    result = {"status": "done", "count": counter.count, "elapsed": round(elapsed, 2)}
    ckpt.save(result)
    return result


# =============================================================
# Task 2: Orbit / Stabilizer Analysis (PARALLEL)
# =============================================================

def _task2_check_chunk(args):
    """Worker: check a chunk of structural transforms for stabilizer membership."""
    chunk_idx, chunk_size, grid = args
    stabilizer_hits = []
    gen = enumerate_structural_transforms()
    for _ in range(chunk_idx * chunk_size):
        next(gen)
    for i, (row_perm, col_perm, rot) in enumerate(gen):
        if i >= chunk_size:
            break
        transformed = apply_structural_transform(grid, row_perm, col_perm, rot)
        is_relab, d_map, c_map = check_is_relabeling(grid, transformed)
        if is_relab:
            stabilizer_hits.append({
                "row_perm": list(row_perm), "col_perm": list(col_perm), "rotation": rot,
                "digit_map": {str(k): v for k, v in d_map.items()},
                "color_map": {str(k): v for k, v in c_map.items()},
            })
    return (chunk_idx, chunk_size, stabilizer_hits)


def task2_orbit_analysis(progress_callback=None, stop_event=None):
    ckpt = CheckpointManager("task2_orbit_analysis")
    saved = ckpt.load()
    if saved and saved.get("status") == "done":
        return saved

    LOG.add("task2", "=== ORBIT ANALYSIS 9x9 (parallel) ===", level="success")
    LOG.add("task2", "Reference grid G0: a known valid Suirodoku 9x9")
    LOG.add("task2", "--- Symmetry group ---", level="math")
    LOG.add("task2", "G_struct = structural Sudoku permutations:", level="math")
    LOG.add("task2", f"  |G_struct| = 216*216*6*6*2 = {TOTAL_STRUCTURAL:,}", level="math")
    LOG.add("task2", "G_full = G_struct * S9(digits) * S9(colors)", level="math")
    full_group = TOTAL_STRUCTURAL * math.factorial(9) * math.factorial(9)
    LOG.add("task2", f"|G_full| = {full_group:.6e}", level="math")
    LOG.add("task2", "Orbit-stabilizer theorem: |Orbit| = |G_full| / |Stab(grid)|", level="math")

    n_workers = max(1, mp.cpu_count() - 1)
    chunk_size = TOTAL_STRUCTURAL // n_workers + 1
    n_chunks = (TOTAL_STRUCTURAL + chunk_size - 1) // chunk_size
    LOG.add("task2", f"Workers: {n_workers}, chunks: {n_chunks}", level="info")

    start_time = time.time()
    args_list = [(i, chunk_size, MODEL_GRID) for i in range(n_chunks)]

    pool = mp.Pool(processes=n_workers)
    stabilizer_count = 0
    stabilizer_examples = []
    checked = 0
    try:
        for chunk_idx, count, hits in pool.imap_unordered(_task2_check_chunk, args_list):
            checked += count
            stabilizer_count += len(hits)
            stabilizer_examples.extend(hits[:50 - len(stabilizer_examples)])
            pct = round(100 * checked / TOTAL_STRUCTURAL, 1)
            rate = checked / (time.time() - start_time + 0.01)
            LOG.add("task2", f"[{pct}%] {checked:,}/{TOTAL_STRUCTURAL:,} -- stab={stabilizer_count}")
            if progress_callback:
                eta = (TOTAL_STRUCTURAL - checked) / rate if rate > 0 else 0
                progress_callback({
                    "checked": checked, "total": TOTAL_STRUCTURAL, "percent": pct,
                    "stabilizer_count": stabilizer_count, "rate": round(rate), "eta_seconds": round(eta),
                })
    finally:
        pool.terminate()
        pool.join()

    elapsed = time.time() - start_time
    orbit_size = full_group // stabilizer_count if stabilizer_count > 0 else 0

    LOG.add("task2", "====================================", level="success")
    LOG.add("task2", f"DONE in {elapsed:.1f}s ({elapsed/60:.1f} min)", level="success")
    LOG.add("task2", f"|Stab| = {stabilizer_count}", level="success")
    LOG.add("task2", f"|Orbit| = {full_group:.6e} / {stabilizer_count} = {orbit_size:.6e}", level="math")
    LOG.add("task2", f"-> G0 orbit: ~{orbit_size:.2e} distinct grids", level="success")

    export_data = {
        "description": "Orbit analysis of reference Suirodoku grid G0",
        "model_grid": grid_to_display(MODEL_GRID),
        "structural_group_order": TOTAL_STRUCTURAL,
        "full_group_order": full_group,
        "stabilizer_order": stabilizer_count,
        "orbit_size": orbit_size,
        "stabilizer_elements": stabilizer_examples,
        "timestamp": datetime.now().isoformat(),
    }
    with open(EXPORT_DIR / "task2_orbit_analysis.json", "w") as f:
        json.dump(export_data, f, indent=2, default=str)
    LOG.add("task2", "Export -> exports/task2_orbit_analysis.json")

    result = {
        "status": "done", "checked": checked, "total": TOTAL_STRUCTURAL,
        "stabilizer_count": stabilizer_count, "stabilizer_examples": stabilizer_examples[:10],
        "full_group_order": full_group, "orbit_size": orbit_size, "elapsed": round(elapsed, 1),
    }
    ckpt.save(result)
    return result


# =============================================================
# Task 3: CP-SAT Grid Search (unbiased)
# =============================================================

def _task3_solve_one(args):
    """Worker: build and solve one CSP.
    Symmetry breaking: digit[0][c]=c AND color[0][c]=c (eliminates 9!² relabelings).
    No rotation constraint — finds ALL types of grids without bias.
    Timeout 37s."""
    from ortools.sat.python import cp_model
    seed, attempt_num, n_threads = args

    rng = random.Random(seed)
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

    # Symmetry breaking: first row fully fixed -> eliminates 9!² relabelings
    # + all column/stack perms + rotation (they'd break row 0)
    for c in range(n):
        model.Add(digit[0][c] == c)
        model.Add(color[0][c] == c)

    # Hierarchical symmetry breaking on rows (144 remaining symmetries -> 1)
    # Column 0 has AllDifferent, so digit[r][0] are all distinct -> safe to use <
    #
    # 1. Rows in band 0: row 0 is fixed, break swap row1↔row2
    model.Add(digit[1][0] < digit[2][0])
    # 2. Order rows within band 1 by digit[r][0]
    model.Add(digit[3][0] < digit[4][0])
    model.Add(digit[4][0] < digit[5][0])
    # 3. Order rows within band 2 by digit[r][0]
    model.Add(digit[6][0] < digit[7][0])
    model.Add(digit[7][0] < digit[8][0])
    # 4. Break band1↔band2 swap: compare first (smallest) row of each band
    model.Add(digit[3][0] < digit[6][0])

    # Random hint to diversify search
    hint_row = rng.choice([1, 3, 6])
    hint_col = rng.randint(1, n-1)
    hint_val = rng.randint(0, n-1)
    model.AddHint(digit[hint_row][hint_col], hint_val)

    solver = cp_model.CpSolver()
    solver.parameters.num_workers = n_threads
    solver.parameters.random_seed = seed
    solver.parameters.max_time_in_seconds = 120

    solve_start = time.time()
    status = solver.Solve(model)
    total_time = round(time.time() - solve_start, 2)

    if status in (cp_model.OPTIMAL, cp_model.FEASIBLE):
        grid = tuple(
            tuple((solver.Value(digit[r][c]), solver.Value(color[r][c])) for c in range(n))
            for r in range(n)
        )
        h = hashlib.md5(str(grid).encode()).hexdigest()
        checks = verify_suirodoku(grid)
        return {
            "status": "found", "grid": grid, "hash": h, "checks": checks,
            "seed": seed, "attempt": attempt_num, "solve_time": total_time,
        }
    else:
        return {
            "status": "timeout", "seed": seed, "attempt": attempt_num,
            "solve_time": total_time,
        }


def task3_search_new_grids(progress_callback=None, stop_event=None, max_attempts=None):
    global TASK3_TARGET
    from ortools.sat.python import cp_model

    # Pre-flight check: C library required for inline canonicalization
    if not canon_lib_available():
        msg = ("canon.dll/.so not found! Compile it first:\n"
               "  Windows: gcc -O3 -shared -o canon.dll canon.c\n"
               "  Linux:   gcc -O3 -shared -fPIC -o canon.so canon.c")
        LOG.add("task3", f"ERROR: {msg}", level="error")
        return {"status": "error", "error": msg}

    if max_attempts is not None:
        TASK3_TARGET = max_attempts

    ckpt = CheckpointManager("task3_search_grids")
    saved = ckpt.load()

    attempts = 0
    orbits = {}          # canon_hash -> orbit info dict
    known_orbits = set() # set of canon_hashes for fast lookup
    found_grids = []     # legacy: raw grids for backward compat
    failed_attempts = 0
    total_solves = 0     # grids found by solver (before dedup)
    swap_bonus = 0       # extra orbits found via swap

    if saved and saved.get("status") in ("running", "done"):
        attempts = saved.get("attempts", 0)
        orbits = saved.get("orbits", {})
        known_orbits = set(orbits.keys())
        found_grids = saved.get("found_grids", [])
        failed_attempts = saved.get("failed_attempts", 0)
        total_solves = saved.get("total_solves", 0)
        swap_bonus = saved.get("swap_bonus", 0)
        if saved.get("status") == "done":
            if attempts >= TASK3_TARGET:
                return saved
            LOG.add("task3", f"Reopening: was done at {attempts}, new target {TASK3_TARGET}", level="warning")

    n_workers = max(1, mp.cpu_count() // 8)  # 2 parallel solvers on 16-core
    threads_per_worker = max(2, mp.cpu_count() // n_workers)  # 8 threads each

    LOG.add("task3", "=== SEARCH FOR 9x9 GRIDS (unbiased, parallel) ===", level="success")
    LOG.add("task3", f"Workers: {n_workers} parallel solvers x {threads_per_worker} threads each", level="info")
    LOG.add("task3", "  Symmetry breaking: digit[0][c] = c, color[0][c] = c + row ordering", level="math")
    LOG.add("task3", "  Inline canonicalization (C) + swap dedup", level="math")

    if attempts > 0:
        LOG.add("task3", f"RESUME: {attempts} attempts, {len(orbits)} orbits, {failed_attempts} failed", level="warning")

    start_time = time.time()
    last_save = time.time()
    batch_size = n_workers * 2

    pool = mp.Pool(processes=n_workers)
    try:
        while attempts < TASK3_TARGET:
            if stop_event and stop_event.is_set():
                break

            current_target = TASK3_TARGET
            batch_count = min(batch_size, current_target - attempts)
            batch_args = []
            for _ in range(batch_count):
                attempts += 1
                seed = random.randint(0, 2**31 - 1)
                batch_args.append((seed, attempts, threads_per_worker))

            for res in pool.imap_unordered(_task3_solve_one, batch_args):
                if res["status"] == "found":
                    if not res["checks"]["all_valid"]:
                        LOG.add("task3", f"X Attempt {res['attempt']}: INVALID grid!", level="error")
                        failed_attempts += 1
                        continue

                    total_solves += 1
                    grid = res["grid"]

                    # Canonicalize via C (fast: ~0.2s)
                    try:
                        canon_hash, canon_flat, stab = fast_canonicalize_and_stab(grid)
                    except Exception as e:
                        LOG.add("task3", f"ERROR canonicalizing: {e}", level="error")
                        failed_attempts += 1
                        continue

                    if canon_hash not in known_orbits:
                        known_orbits.add(canon_hash)
                        grid_display = grid_to_display(grid)
                        orbit_data = {
                            "canon_hash": canon_hash,
                            "canon_flat": list(canon_flat),
                            "stab_size": stab,
                            "grid": grid_display,
                            "found_at_attempt": res["attempt"],
                            "solve_time": res["solve_time"],
                            "source": "solver",
                        }

                        # If stab > 1, compute detailed symmetry types (rare, ~80s)
                        if stab > 1:
                            try:
                                LOG.add("task3", f"  Analyzing symmetries (|Stab|={stab})...", level="math")
                                _, fixes = compute_detailed_stabilizer(grid)
                                sym_types = [f["description"] for f in fixes]
                                orbit_data["symmetry_types"] = sym_types
                                orbit_data["symmetry_fixes"] = [{
                                    "description": f["description"],
                                    "rot": f["rot"],
                                    "row_perm": f["row_perm"],
                                    "col_perm": f["col_perm"],
                                } for f in fixes]
                                LOG.add("task3", f"  Symmetries: {sym_types}", level="math")
                            except Exception as e:
                                LOG.add("task3", f"  Symmetry analysis failed: {e}", level="error")

                        orbits[canon_hash] = orbit_data
                        # Legacy compat
                        found_grids.append({
                            "grid_hash": res["hash"],
                            "canon_hash": canon_hash,
                            "grid": grid_display,
                            "found_at_attempt": res["attempt"],
                            "solve_time": res["solve_time"],
                            "seed": res["seed"],
                            "verification": res["checks"],
                        })
                        LOG.add("task3",
                            f"★ ORBIT #{len(orbits)} (attempt {res['attempt']}, "
                            f"{res['solve_time']:.1f}s) -- |Stab|={stab}",
                            level="success")
                    else:
                        LOG.add("task3", f"~ Attempt {res['attempt']}: known orbit {canon_hash[:8]}", level="warning")

                    # Apply swap(G) and canonicalize -> maybe a free new orbit
                    try:
                        swapped = swap_grid(grid)
                        swap_hash, swap_flat, swap_stab = fast_canonicalize_and_stab(swapped)
                    except Exception as e:
                        LOG.add("task3", f"ERROR swap-canonicalizing: {e}", level="error")
                        swapped = None

                    if swapped is not None and swap_hash not in known_orbits:
                        known_orbits.add(swap_hash)
                        swap_display = grid_to_display(swapped)
                        swap_orbit_data = {
                            "canon_hash": swap_hash,
                            "canon_flat": list(swap_flat),
                            "stab_size": swap_stab,
                            "grid": swap_display,
                            "found_at_attempt": res["attempt"],
                            "solve_time": 0,
                            "source": "swap",
                        }

                        if swap_stab > 1:
                            try:
                                LOG.add("task3", f"  Analyzing swap symmetries (|Stab|={swap_stab})...", level="math")
                                _, fixes = compute_detailed_stabilizer(swapped)
                                sym_types = [f["description"] for f in fixes]
                                swap_orbit_data["symmetry_types"] = sym_types
                                swap_orbit_data["symmetry_fixes"] = [{
                                    "description": f["description"],
                                    "rot": f["rot"],
                                    "row_perm": f["row_perm"],
                                    "col_perm": f["col_perm"],
                                } for f in fixes]
                                LOG.add("task3", f"  Swap symmetries: {sym_types}", level="math")
                            except Exception as e:
                                LOG.add("task3", f"  Swap symmetry analysis failed: {e}", level="error")

                        orbits[swap_hash] = swap_orbit_data
                        found_grids.append({
                            "grid_hash": hashlib.md5(str(swapped).encode()).hexdigest(),
                            "canon_hash": swap_hash,
                            "grid": swap_display,
                            "found_at_attempt": res["attempt"],
                            "solve_time": 0,
                            "seed": res["seed"],
                            "verification": verify_suirodoku(swapped),
                        })
                        swap_bonus += 1
                        LOG.add("task3",
                            f"★ SWAP BONUS orbit #{len(orbits)} -- |Stab|={swap_stab}",
                            level="success")

                else:
                    failed_attempts += 1
                    LOG.add("task3", f"X Attempt {res['attempt']}: timeout ({res['solve_time']:.1f}s)", level="warning")

                now = time.time()
                if progress_callback:
                    elapsed = now - start_time
                    progress_callback({
                        "attempts": attempts, "max_attempts": TASK3_TARGET,
                        "found": len(found_grids), "orbits": len(orbits),
                        "failed": failed_attempts, "swap_bonus": swap_bonus,
                        "total_solves": total_solves,
                        "percent": round(100 * attempts / max(1, TASK3_TARGET), 1),
                        "success_rate": round(100 * total_solves / attempts, 1) if attempts > 0 else 0,
                        "last_solve_time": res.get("solve_time", 0),
                    })
                if now - last_save > 5.0:
                    last_save = now
                    ckpt.save({
                        "status": "running", "attempts": attempts, "max_attempts": TASK3_TARGET,
                        "orbits": orbits, "found_grids": found_grids,
                        "known_orbits": list(known_orbits),
                        "failed_attempts": failed_attempts,
                        "total_solves": total_solves, "swap_bonus": swap_bonus,
                        "elapsed": round(now - start_time, 1),
                    })
    finally:
        pool.terminate()
        pool.join()

    elapsed = time.time() - start_time
    sr = round(100 * total_solves / attempts, 1) if attempts > 0 else 0
    dup_rate = round(100 * (1 - len(orbits) / max(1, total_solves + swap_bonus)), 1)

    # Stab distribution
    stab_dist = {}
    for o in orbits.values():
        s = str(o["stab_size"])
        stab_dist[s] = stab_dist.get(s, 0) + 1

    LOG.add("task3", "====================================", level="success")
    LOG.add("task3", f"DONE: {len(orbits)} distinct orbits / {attempts} attempts ({sr}% solve rate)", level="success")
    LOG.add("task3", f"  Solver found: {total_solves} grids, Swap bonus: {swap_bonus}", level="info")
    LOG.add("task3", f"  Duplicate rate: {dup_rate}%", level="info")
    LOG.add("task3", f"  Stab distribution: {stab_dist}", level="math")
    LOG.add("task3", f"  Failed (timeout/invalid): {failed_attempts}", level="info")
    LOG.add("task3", f"  Total time: {elapsed:.1f}s ({elapsed/60:.1f} min)", level="info")

    export_data = {
        "description": "Suirodoku 9x9 orbits found by CP-SAT solver (unbiased, inline canonicalization)",
        "method": {
            "solver": "Google OR-Tools CP-SAT",
            "encoding": "Pair encoding: k = digit * 9 + color",
            "constraints": "55 AllDifferent + 6 row-ordering (symmetry breaking)",
            "symmetry_breaking": "digit[0][c]=c, color[0][c]=c, row ordering in col 0 (eliminates 9!² × 144 symmetries)",
            "canonicalization": "C library (3,359,232 transforms), inline per grid",
            "swap": "d↔k swap applied to each grid for free orbit discovery",
            "timeout_per_attempt": 120,
        },
        "statistics": {
            "attempts": attempts, "orbits": len(orbits),
            "total_solves": total_solves, "swap_bonus": swap_bonus,
            "failed": failed_attempts, "duplicate_rate": dup_rate,
            "success_rate": sr, "total_time_seconds": round(elapsed, 1),
            "stab_distribution": stab_dist,
        },
        "grids": found_grids,
        "timestamp": datetime.now().isoformat(),
    }
    with open(EXPORT_DIR / "task3_grids.json", "w") as f:
        json.dump(export_data, f, indent=2, default=str)
    LOG.add("task3", "Export -> exports/task3_grids.json")

    result = {
        "status": "done", "attempts": attempts, "max_attempts": TASK3_TARGET,
        "orbits": orbits, "found_grids": found_grids,
        "total_distinct": len(orbits), "n_orbits": len(orbits),
        "total_solves": total_solves, "swap_bonus": swap_bonus,
        "failed_attempts": failed_attempts, "known_orbits": list(known_orbits),
        "stab_distribution": stab_dist,
        "elapsed": round(elapsed, 1),
    }
    ckpt.save(result)
    return result


# =============================================================
# Task 5: Minimum Clue Problem 4x4 -- ALL 2304 + Variant 2
# =============================================================

def enumerate_4x4_grids():
    """Enumerate all Suirodoku 4x4 grids (2304)."""
    from ortools.sat.python import cp_model

    class AllSolCollector(cp_model.CpSolverSolutionCallback):
        def __init__(self, digit_vars, color_vars, n):
            super().__init__()
            self.solutions = []
            self.digit_vars = digit_vars
            self.color_vars = color_vars
            self.n = n
        def on_solution_callback(self):
            grid = tuple(
                tuple((self.Value(self.digit_vars[r][c]),
                       self.Value(self.color_vars[r][c]))
                      for c in range(self.n))
                for r in range(self.n)
            )
            self.solutions.append(grid)

    n, b = 4, 2
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

    solver = cp_model.CpSolver()
    solver.parameters.enumerate_all_solutions = True
    solver.parameters.num_workers = 1
    collector = AllSolCollector(digit, color, n)
    solver.Solve(model, collector)
    return collector.solutions


def count_solutions_4x4(clues):
    """Count solutions with given clues. Returns 0, 1, or 2.
    clues = {(r,c): (d, k)} for full pair,
    or {(r,c): ('d', val)} for digit-only, {(r,c): ('k', val)} for color-only."""
    from ortools.sat.python import cp_model

    class Counter(cp_model.CpSolverSolutionCallback):
        def __init__(self):
            super().__init__()
            self.count = 0
        def on_solution_callback(self):
            self.count += 1
            if self.count >= 2:
                self.StopSearch()

    n, b = 4, 2
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

    for (r, c), val in clues.items():
        if isinstance(val, tuple) and len(val) == 2:
            if val[0] == 'd':
                model.Add(digit[r][c] == val[1])
            elif val[0] == 'k':
                model.Add(color[r][c] == val[1])
            else:
                model.Add(digit[r][c] == val[0])
                model.Add(color[r][c] == val[1])

    solver = cp_model.CpSolver()
    solver.parameters.enumerate_all_solutions = True
    solver.parameters.num_workers = 1
    counter = Counter()
    solver.Solve(model, counter)
    return counter.count


def _greedy_removal_4x4(grid, n_shuffles=3):
    """Try greedy removal with multiple random orderings, return best clue set."""
    best_clues = None
    for _ in range(n_shuffles):
        clues = {(r, c): grid[r][c] for r in range(4) for c in range(4)}
        cells = list(clues.keys())
        random.shuffle(cells)
        for cell in cells:
            trial = dict(clues)
            del trial[cell]
            if count_solutions_4x4(trial) == 1:
                clues = trial
        if best_clues is None or len(clues) < len(best_clues):
            best_clues = clues
    return best_clues


def _task5_process_one(args):
    """Worker for multiprocessing: process one grid for min clue analysis."""
    idx, grid = args
    clues_v1 = _greedy_removal_4x4(grid, n_shuffles=3)
    n_v1 = len(clues_v1)
    # V2: degrade
    clues_v2 = dict(clues_v1)
    for cell in list(clues_v2.keys()):
        trial = dict(clues_v2)
        del trial[cell]
        if count_solutions_4x4(trial) == 1:
            clues_v2 = trial
    for cell in list(clues_v2.keys()):
        d, k = clues_v2[cell]
        trial = dict(clues_v2)
        trial[cell] = ('d', d)
        if count_solutions_4x4(trial) == 1:
            clues_v2 = trial
            continue
        trial = dict(clues_v2)
        trial[cell] = ('k', k)
        if count_solutions_4x4(trial) == 1:
            clues_v2 = trial
    n_v2 = len(clues_v2)
    return (idx, n_v1, n_v2, [(r,c) for r,c in clues_v1.keys()])


def task5_minimum_clue_4x4(progress_callback=None, stop_event=None):
    """Find minimum clues for unique Suirodoku 4x4 puzzles.
    Variant 1: each clue = full pair (digit + color).
    Variant 2: clues can be digit-only, color-only, or pair (heuristic)."""

    ckpt = CheckpointManager("task5_min_clues_4x4")
    saved = ckpt.load()
    if saved and saved.get("status") == "done":
        return saved

    LOG.add("task5", "=== MINIMUM CLUE SUIRODOKU 4x4 ===", level="success")
    LOG.add("task5", "Variant 1: clue = full pair (digit + color)", level="math")
    LOG.add("task5", "Variant 2: clue = digit only / color only / pair", level="math")
    LOG.add("task5", "EXHAUSTIVE processing of all 2304 grids (3 greedy shuffles each)", level="math")

    start_idx = 0
    global_min_v1 = 16
    global_min_v2 = 16
    best_v1 = []
    best_v2 = []
    all_v1 = []

    if saved and saved.get("status") == "running":
        start_idx = saved.get("processed", 0)
        global_min_v1 = saved.get("global_min_v1", 16)
        global_min_v2 = saved.get("global_min_v2", 16)
        best_v1 = saved.get("best_v1", [])
        best_v2 = saved.get("best_v2", [])
        all_v1 = saved.get("all_v1", [])
        LOG.add("task5", f"RESUME: {start_idx} done, V1 min={global_min_v1}, V2 min={global_min_v2}", level="warning")

    LOG.add("task5", "Enumerating all 2304 grids 4x4...")
    grids = enumerate_4x4_grids()
    LOG.add("task5", f"Grids enumerated: {len(grids)}", level="success")

    start_time = time.time()
    last_save = time.time()

    remaining = [(i, grids[i]) for i in range(start_idx, len(grids))]
    n_workers = max(1, mp.cpu_count() - 1)
    LOG.add("task5", f"Processing {len(remaining)} grids on {n_workers} cores...", level="info")

    pool = mp.Pool(processes=n_workers)
    try:
        for idx, n_v1, n_v2, clue_cells in pool.imap_unordered(_task5_process_one, remaining):
            all_v1.append(n_v1)
            if n_v1 < global_min_v1:
                global_min_v1 = n_v1
                best_v1 = [{"grid_idx": idx, "n_clues": n_v1, "clue_cells": clue_cells}]
                LOG.add("task5", f"* V1 NEW MIN: {n_v1} clues (grid #{idx})", level="success")
            elif n_v1 == global_min_v1 and len(best_v1) < 20:
                best_v1.append({"grid_idx": idx, "n_clues": n_v1, "clue_cells": clue_cells})
            if n_v2 < global_min_v2:
                global_min_v2 = n_v2
                best_v2 = [{"grid_idx": idx, "n_clues": n_v2}]
                LOG.add("task5", f"* V2 NEW MIN: {n_v2} partial clues (grid #{idx})", level="success")
            elif n_v2 == global_min_v2 and len(best_v2) < 20:
                best_v2.append({"grid_idx": idx, "n_clues": n_v2})
            if len(all_v1) % 100 == 0:
                LOG.add("task5", f"Grid {len(all_v1)}/{len(grids)} -- V1 min={global_min_v1}, V2 min={global_min_v2}")
            if progress_callback:
                progress_callback({
                    "processed": len(all_v1), "total": len(grids),
                    "global_min_v1": global_min_v1, "global_min_v2": global_min_v2,
                    "percent": round(100 * len(all_v1) / len(grids), 1),
                })
            now = time.time()
            if now - last_save > 5.0:
                last_save = now
                ckpt.save({
                    "status": "running", "processed": len(all_v1), "total": len(grids),
                    "global_min_v1": global_min_v1, "global_min_v2": global_min_v2,
                    "best_v1": best_v1, "best_v2": best_v2, "all_v1": all_v1,
                })
    finally:
        pool.terminate()
        pool.join()

    elapsed = time.time() - start_time

    from collections import Counter as PyCounter
    dist_v1 = dict(PyCounter(all_v1))

    LOG.add("task5", "====================================", level="success")
    LOG.add("task5", f"DONE: {len(all_v1)} grids in {elapsed:.1f}s ({elapsed/60:.1f} min)", level="success")
    LOG.add("task5", f"VARIANT 1 (full pairs): minimum = {global_min_v1} clues", level="success")
    LOG.add("task5", f"VARIANT 2 (partial clues): minimum = {global_min_v2} clues", level="success")
    LOG.add("task5", f"V1 distribution: {dist_v1}", level="math")
    LOG.add("task5", f"Comparison: Sudoku 4x4 = 4 clues minimum", level="math")

    export_data = {
        "description": "Minimum clue problem for Suirodoku 4x4",
        "method": "Exhaustive 2304 grids, greedy removal (3 shuffles) + V2 partial degradation",
        "variant1_full_pairs": {
            "minimum_clues": global_min_v1, "distribution": dist_v1,
            "best_puzzles": best_v1[:20],
        },
        "variant2_partial_clues": {
            "minimum_clues": global_min_v2, "best_puzzles": best_v2[:20],
            "note": "Heuristic: greedy removal then degrade pairs to digit-only or color-only",
        },
        "comparison_sudoku_4x4": 4,
        "grids_tested": len(all_v1),
        "timestamp": datetime.now().isoformat(),
    }
    with open(EXPORT_DIR / "task5_minimum_clues_4x4.json", "w") as f:
        json.dump(export_data, f, indent=2, default=str)
    LOG.add("task5", "Export -> exports/task5_minimum_clues_4x4.json")

    result = {
        "status": "done", "processed": len(all_v1), "total": len(grids),
        "global_min_v1": global_min_v1, "global_min_v2": global_min_v2,
        "distribution": dist_v1, "elapsed": round(elapsed, 1),
    }
    ckpt.save(result)
    return result


# =============================================================
# Task 6: Color Mate Count -- deduplicated by digit layer
# =============================================================

def count_color_mates(sudoku_digits, max_solutions=10000, timeout=60):
    """Given a complete Sudoku (digits only), count compatible color layers."""
    from ortools.sat.python import cp_model

    class MateCounter(cp_model.CpSolverSolutionCallback):
        def __init__(self, max_sol):
            super().__init__()
            self.count = 0
            self.max_sol = max_sol
        def on_solution_callback(self):
            self.count += 1
            if self.count >= self.max_sol:
                self.StopSearch()

    n, b = 9, 3
    model = cp_model.CpModel()
    color = [[model.NewIntVar(0, n-1, f'k_{r}_{c}') for c in range(n)] for r in range(n)]

    for r in range(n):
        model.AddAllDifferent(color[r])
    for c in range(n):
        model.AddAllDifferent([color[r][c] for r in range(n)])
    for br in range(b):
        for bc in range(b):
            model.AddAllDifferent([color[r][c]
                for r in range(br*b, (br+1)*b)
                for c in range(bc*b, (bc+1)*b)])

    pair = [[model.NewIntVar(0, n*n-1, f'p_{r}_{c}') for c in range(n)] for r in range(n)]
    for r in range(n):
        for c in range(n):
            d = sudoku_digits[r][c]
            model.Add(pair[r][c] == d * n + color[r][c])
    model.AddAllDifferent([pair[r][c] for r in range(n) for c in range(n)])

    solver = cp_model.CpSolver()
    solver.parameters.enumerate_all_solutions = True
    solver.parameters.num_workers = 4
    solver.parameters.max_time_in_seconds = timeout

    counter = MateCounter(max_solutions)
    solver.Solve(model, counter)
    return counter.count


def _task6_count_one(args):
    """Worker for multiprocessing: count color mates for one Sudoku."""
    idx, digit_layer, grid_indices = args
    t0 = time.time()
    n_mates = count_color_mates(digit_layer, max_solutions=100, timeout=30)
    dt = time.time() - t0
    return {
        "sudoku_idx": idx, "n_mates": n_mates,
        "time": round(dt, 2), "source_grids": grid_indices,
    }


def task6_mate_count(progress_callback=None, stop_event=None):
    """For each UNIQUE digit layer from Task 03 grids, count color mates.
    Deduplicates: multiple Suirodoku may share the same digit layer."""

    ckpt = CheckpointManager("task6_mate_count")
    saved = ckpt.load()
    if saved and saved.get("status") == "done":
        return saved

    ckpt3 = CheckpointManager("task3_search_grids")
    saved3 = ckpt3.load()
    if not saved3 or "found_grids" not in saved3:
        LOG.add("task6", "ERROR: no Task 03 grids. Run Task 03 first.", level="error")
        return {"status": "error", "error": "No Task 03 data"}

    raw_grids = saved3["found_grids"]

    # Deduplicate by digit layer
    seen_digits = {}
    for i, g_data in enumerate(raw_grids):
        digit_layer = tuple(
            tuple(cell["d"] - 1 for cell in row)
            for row in g_data["grid"]
        )
        dhash = hashlib.md5(str(digit_layer).encode()).hexdigest()
        if dhash not in seen_digits:
            seen_digits[dhash] = (digit_layer, [])
        seen_digits[dhash][1].append(i)

    unique_sudokus = list(seen_digits.values())

    LOG.add("task6", "=== COLOR MATE COUNT ===", level="success")
    LOG.add("task6", f"Task 03 grids: {len(raw_grids)} -- Unique Sudoku (digit layer): {len(unique_sudokus)}", level="math")
    LOG.add("task6", "For each unique Sudoku: enumerate color mates (max 100, timeout 30s)", level="math")

    start_idx = 0
    mate_counts = []

    if saved and saved.get("status") == "running":
        start_idx = saved.get("processed", 0)
        mate_counts = saved.get("mate_counts", [])
        LOG.add("task6", f"RESUME: {start_idx} already processed", level="warning")

    start_time = time.time()
    last_save = time.time()

    remaining = [(i, unique_sudokus[i][0], unique_sudokus[i][1])
                 for i in range(start_idx, len(unique_sudokus))]
    n_workers = max(1, mp.cpu_count() // 4)  # each solver uses 4 threads
    LOG.add("task6", f"Processing {len(remaining)} Sudoku on {n_workers} cores...", level="info")

    pool = mp.Pool(processes=n_workers)
    try:
        for result in pool.imap_unordered(_task6_count_one, remaining):
            mate_counts.append(result)
            n_mates = result["n_mates"]
            i = result["sudoku_idx"]
            dt = result["time"]
            if n_mates == 1:
                LOG.add("task6", f"Sudoku {len(mate_counts)}/{len(unique_sudokus)}: 1 unique mate ({dt:.1f}s)")
            elif n_mates >= 100:
                LOG.add("task6", f"* Sudoku {len(mate_counts)}/{len(unique_sudokus)}: >={n_mates} mates! ({dt:.1f}s)", level="success")
            else:
                LOG.add("task6", f"Sudoku {len(mate_counts)}/{len(unique_sudokus)}: {n_mates} mates ({dt:.1f}s)")
            if progress_callback:
                counts = [m["n_mates"] for m in mate_counts]
                avg = sum(counts) / len(counts) if counts else 0
                progress_callback({
                    "processed": len(mate_counts), "total": len(unique_sudokus),
                    "percent": round(100 * len(mate_counts) / len(unique_sudokus), 1),
                    "avg_mates": round(avg, 2), "last_count": n_mates,
                })
            now = time.time()
            if now - last_save > 5.0:
                last_save = now
                ckpt.save({
                    "status": "running", "processed": len(mate_counts),
                    "total": len(unique_sudokus), "mate_counts": mate_counts,
                })
    finally:
        pool.terminate()
        pool.join()

    elapsed = time.time() - start_time
    counts_only = [m["n_mates"] for m in mate_counts]
    avg_mates = sum(counts_only) / len(counts_only) if counts_only else 0
    max_mates = max(counts_only) if counts_only else 0
    min_mates = min(counts_only) if counts_only else 0
    n_unique = sum(1 for c in counts_only if c == 1)

    from collections import Counter as PyCounter
    dist = dict(PyCounter(counts_only))

    LOG.add("task6", "====================================", level="success")
    LOG.add("task6", f"DONE: {len(mate_counts)} unique Sudoku in {elapsed:.1f}s", level="success")
    LOG.add("task6", f"Mates: min={min_mates}, max={max_mates}, avg={avg_mates:.1f}", level="success")
    LOG.add("task6", f"Unique mate: {n_unique}/{len(mate_counts)} ({100*n_unique/len(mate_counts):.1f}%)", level="math")
    LOG.add("task6", f"Distribution: {dist}", level="math")

    ckpt4 = CheckpointManager("task4_orbits")
    saved4 = ckpt4.load()
    task4_summary = None
    if saved4 and saved4.get("status") == "done":
        task4_summary = {
            "n_orbits": saved4.get("n_orbits"),
            "total_suirodoku_lower_bound": saved4.get("total_suirodoku_lower_bound"),
        }

    export_data = {
        "description": "Color mate distribution per unique digit layer",
        "method": "Deduplicate digit layers, enumerate mates via CP-SAT (cap 10000, timeout 60s)",
        "results": {
            "total_suirodoku_grids": len(raw_grids),
            "unique_digit_layers": len(unique_sudokus),
            "avg_mates": round(avg_mates, 2),
            "min_mates": min_mates, "max_mates": max_mates,
            "unique_mate_count": n_unique, "distribution": dist,
        },
        "task4_context": task4_summary,
        "mate_counts": mate_counts,
        "timestamp": datetime.now().isoformat(),
    }
    with open(EXPORT_DIR / "task6_mates_distribution.json", "w") as f:
        json.dump(export_data, f, indent=2, default=str)
    LOG.add("task6", "Export -> exports/task6_mates_distribution.json")

    result = {
        "status": "done", "processed": len(mate_counts),
        "total": len(unique_sudokus),
        "avg_mates": round(avg_mates, 2), "min_mates": min_mates,
        "max_mates": max_mates, "unique_mate_count": n_unique,
        "distribution": dist, "elapsed": round(elapsed, 1),
    }
    ckpt.save(result)
    return result


# =============================================================
# Task 7: Minimum Clue Problem 9×9 (Variant A: clue = full pair)
# =============================================================

def has_unique_solution_9x9(clues, timeout=10):
    """Check if a set of clues yields exactly 1 Suirodoku solution.
    No rotation constraint — checks ALL valid completions.
    clues = {(r,c): (digit, color)} where digit,color are 0-indexed.
    Returns: 0 (no solution), 1 (unique), or 2+ (ambiguous)."""
    from ortools.sat.python import cp_model

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


def _greedy_removal_9x9(grid, n_shuffles=5, timeout_per_check=10):
    """Greedy removal: start with all 81 clues, remove one at a time.
    Try n_shuffles random orderings, keep the best (fewest clues).
    grid = tuple of tuples ((digit, color), ...) 0-indexed."""
    best_clues = None
    for shuffle_idx in range(n_shuffles):
        clues = {(r, c): grid[r][c] for r in range(9) for c in range(9)}
        cells = list(clues.keys())
        random.shuffle(cells)
        for cell in cells:
            trial = dict(clues)
            del trial[cell]
            nsol = has_unique_solution_9x9(trial, timeout=timeout_per_check)
            if nsol == 1:
                clues = trial  # safe to remove
        if best_clues is None or len(clues) < len(best_clues):
            best_clues = clues
    return best_clues


def _task7_process_one(args):
    """Worker: process one grid for minimum clue analysis."""
    idx, grid, n_shuffles = args
    best_clues = _greedy_removal_9x9(grid, n_shuffles=n_shuffles)
    n_clues = len(best_clues)
    clue_cells = sorted(best_clues.keys())
    return (idx, n_clues, clue_cells)


def task7_minimum_clue_9x9(progress_callback=None, stop_event=None, selected_hashes=None):
    """Find minimum clue puzzles for Suirodoku 9×9.
    Variant A: each clue = full pair (digit + color).
    Uses orbit representatives from Task 03 (inline canonicalization).
    selected_hashes: list of orbit canonical hashes to process (or None for all)."""

    ckpt = CheckpointManager("task7_min_clues_9x9")
    saved = ckpt.load()

    # Load orbits from Task 3
    ckpt3 = CheckpointManager("task3_search_grids")
    saved3 = ckpt3.load()
    if not saved3:
        LOG.add("task7", "ERROR: no Task 03 data. Run Task 03 first.", level="error")
        return {"status": "error", "error": "No Task 03 data"}

    orbits_dict = saved3.get("orbits", {})
    if not orbits_dict:
        LOG.add("task7", "ERROR: no orbits in Task 03. Run Task 03 first.", level="error")
        return {"status": "error", "error": "No orbits in Task 03"}

    # Parse orbit rep grids
    orbit_grids = {}  # canon_hash -> grid_internal
    for chash, odata in orbits_dict.items():
        gd = odata["grid"]
        grid_internal = tuple(
            tuple((cell["d"] - 1, COLOR_MAP[cell["c"]]) for cell in row)
            for row in gd
        )
        orbit_grids[chash] = grid_internal

    # Build work list
    if selected_hashes:
        work_hashes = [h for h in selected_hashes if h in orbit_grids]
    else:
        work_hashes = list(orbit_grids.keys())

    if not work_hashes:
        LOG.add("task7", "ERROR: no valid orbits selected.", level="error")
        return {"status": "error", "error": "No valid orbits selected"}

    LOG.add("task7", "=== MINIMUM CLUE SUIRODOKU 9×9 ===", level="success")
    LOG.add("task7", "Variant A: clue = full pair (digit + color)", level="math")
    LOG.add("task7", "Method: greedy removal + CP-SAT uniqueness", level="math")
    LOG.add("task7", f"Selected orbits: {len(work_hashes)} / {len(orbits_dict)} total", level="info")
    LOG.add("task7", "5 random shuffles per grid, best kept", level="math")

    # Resume from checkpoint
    global_min = 81
    processed = 0
    results = {}  # keyed by orbit hash
    distribution = {}

    if saved and saved.get("status") in ("running", "done"):
        processed = saved.get("processed", 0)
        global_min = saved.get("global_min", 81)
        results = saved.get("results", {})
        if isinstance(results, list):
            results = {}  # old format, reset
        distribution = saved.get("distribution", {})
        # Filter: only process hashes not yet done
        work_hashes = [h for h in work_hashes if h not in results]
        if not work_hashes and saved.get("status") == "done":
            return saved
        if results:
            LOG.add("task7", f"RESUME: {len(results)} orbits done, min = {global_min}", level="warning")

    total = len(results) + len(work_hashes)
    n_shuffles = 5
    start_time = time.time()
    last_save = time.time()

    for i, chash in enumerate(work_hashes):
        if stop_event and stop_event.is_set():
            break

        grid = orbit_grids[chash]
        orbit_num = list(orbits_dict.keys()).index(chash) + 1

        LOG.add("task7", f"Orbit #{orbit_num} (hash={chash[:8]}...): greedy removal...")

        best_clues = _greedy_removal_9x9(grid, n_shuffles=n_shuffles)
        n_clues = len(best_clues)
        clue_cells = sorted(best_clues.keys())

        processed = len(results) + 1
        dist_key = str(n_clues)
        distribution[dist_key] = distribution.get(dist_key, 0) + 1

        results[chash] = {
            "orbit_hash": chash,
            "orbit_index": orbit_num,
            "n_clues": n_clues,
            "clue_cells": clue_cells,
        }

        if n_clues < global_min:
            global_min = n_clues
            LOG.add("task7", f"*** NEW MIN: {n_clues} clues (orbit #{orbit_num}) ***", level="success")
        else:
            LOG.add("task7", f"Orbit #{orbit_num}: {n_clues} clues (best: {global_min})", level="info")

        elapsed = time.time() - start_time
        avg_time = elapsed / (i + 1)

        if progress_callback:
            progress_callback({
                "processed": len(results), "total": total,
                "global_min": global_min,
                "last_n_clues": n_clues,
                "last_orbit": orbit_num,
                "percent": round(100 * len(results) / total, 1),
                "avg_time_per_orbit": round(avg_time, 1),
                "distribution": distribution,
            })

        now = time.time()
        if now - last_save > 10.0:
            last_save = now
            ckpt.save({
                "status": "running", "processed": len(results),
                "total": total, "global_min": global_min,
                "results": results, "distribution": distribution,
                "elapsed": round(now - start_time, 1),
            })

    if stop_event and stop_event.is_set():
        ckpt.save({
            "status": "running", "processed": len(results),
            "total": total, "global_min": global_min,
            "results": results, "distribution": distribution,
            "elapsed": round(time.time() - start_time, 1),
        })
        return {"status": "paused"}

    elapsed = time.time() - start_time

    LOG.add("task7", "====================================", level="success")
    LOG.add("task7", f"DONE: {len(results)} orbits in {elapsed:.0f}s ({elapsed/60:.1f} min)", level="success")
    LOG.add("task7", f"MINIMUM CLUES (Variant A): {global_min}", level="success")
    LOG.add("task7", f"Distribution: {distribution}", level="math")

    export_data = {
        "description": "Minimum clue analysis for Suirodoku 9×9 (Variant A: full pairs)",
        "method": "Greedy removal + CP-SAT uniqueness, 5 shuffles per orbit rep",
        "orbits_analyzed": len(results),
        "global_minimum": global_min,
        "distribution": distribution,
        "results": results,
        "elapsed": round(elapsed, 1),
        "timestamp": datetime.now().isoformat(),
    }
    with open(EXPORT_DIR / "task7_min_clues_9x9.json", "w") as f:
        json.dump(export_data, f, indent=2, default=str)
    LOG.add("task7", "Export -> exports/task7_min_clues_9x9.json")

    result = {
        "status": "done", "processed": len(results),
        "total": total, "global_min": global_min,
        "results": results, "distribution": distribution,
        "elapsed": round(elapsed, 1),
    }
    ckpt.save(result)
    return result


def task7_parallel(progress_callback=None, stop_event=None, n_workers=16):
    """Parallel minimum clue analysis — launches N workers, each processes a slice of orbits.
    Reads worker logs and forwards progress to dashboard."""

    # Check Task 3 data exists
    ckpt3 = CheckpointManager("task3_search_grids")
    saved3 = ckpt3.load()
    if not saved3 or not saved3.get("orbits"):
        LOG.add("task7", "ERROR: no Task 03 data. Run Task 03 first.", level="error")
        return {"status": "error", "error": "No Task 03 data"}

    n_orbits = len(saved3["orbits"])

    # Check if already merged
    ckpt_merged = CheckpointManager("task7_min_clues_9x9")
    saved_merged = ckpt_merged.load()
    if saved_merged and saved_merged.get("status") == "done" and saved_merged.get("method") == "parallel":
        return saved_merged

    worker_path = Path(__file__).parent / "worker_task7.py"
    if not worker_path.exists():
        LOG.add("task7", f"ERROR: {worker_path} not found", level="error")
        return {"status": "error", "error": "worker_task7.py not found"}

    LOG.add("task7", "=" * 60, level="success")
    LOG.add("task7", f"TASK 7 PARALLEL — {n_workers} workers, {n_orbits} orbits", level="success")
    LOG.add("task7", "=" * 60, level="success")
    LOG.add("task7", "Method: greedy removal + CP-SAT, 5 shuffles/orbit", level="math")

    # Launch all workers
    t0 = time.time()
    procs = {}
    logs_files = {}
    for wid in range(n_workers):
        # Skip if already done
        already = CheckpointManager(f"task7_w{wid}").load()
        if already and already.get("status") == "done":
            LOG.add("task7", f"  W{wid} already done, skip", level="info")
            continue
        log_path = Path(__file__).parent / f"log_task7_w{wid}.txt"
        lf = open(log_path, "w")
        env = os.environ.copy()
        env["PYTHONUNBUFFERED"] = "1"
        proc = subprocess.Popen(
            [sys.executable, str(worker_path), str(wid), str(n_workers)],
            stdout=lf, stderr=subprocess.STDOUT,
            cwd=str(Path(__file__).parent), env=env
        )
        procs[wid] = proc
        logs_files[wid] = lf

    LOG.add("task7", f"Launched {len(procs)} workers", level="info")

    if progress_callback:
        progress_callback({"phase": "parallel", "percent": 2,
                           "message": f"Launched {len(procs)} workers"})

    n_total_workers = n_workers
    log_last_lines = {}  # track last seen line per worker to detect NEW MIN
    last_summary_time = 0
    import re

    def _parse_worker_log(wid):
        """Read last progress line from worker log, return (done, total, best_min, last_line)."""
        try:
            log_path = Path(__file__).parent / f"log_task7_w{wid}.txt"
            with open(log_path, "r") as rf:
                lines = rf.readlines()
            for line in reversed(lines):
                line = line.strip()
                if not line:
                    continue
                # Parse: [W0] 3/38 — 24 clues (best: 22) (0.02/s, ETA 1800s)
                m = re.search(r'(\d+)/(\d+)', line)
                m_best = re.search(r'best:\s*(\d+)', line)
                done = int(m.group(1)) if m else 0
                total = int(m.group(2)) if m else 0
                best = int(m_best.group(1)) if m_best else 81
                return done, total, best, line
        except:
            pass
        return 0, 0, 81, ""

    # Poll until all done
    while procs:
        if stop_event and stop_event.is_set():
            LOG.add("task7", "STOP requested — terminating workers", level="warning")
            for proc in procs.values():
                proc.terminate()
            for lf in logs_files.values():
                lf.close()
            return {"status": "paused"}

        time.sleep(5)

        # Check for important events (NEW MIN) in each worker
        for wid in list(procs.keys()):
            _, _, _, last_line = _parse_worker_log(wid)
            if last_line and last_line != log_last_lines.get(wid):
                old_line = log_last_lines.get(wid, "")
                log_last_lines[wid] = last_line
                if "NEW MIN" in last_line:
                    LOG.add("task7", last_line, level="success")

        # Handle finished workers
        done_keys = [k for k, p in procs.items() if p.poll() is not None]
        for k in done_keys:
            rc = procs[k].returncode
            logs_files[k].close()
            del procs[k]
            del logs_files[k]
            if rc == 0:
                cp = CheckpointManager(f"task7_w{k}").load()
                w_min = cp.get("global_min", "?") if cp else "?"
                w_proc = cp.get("processed", "?") if cp else "?"
                LOG.add("task7", f"W{k} finished ✓ ({w_proc} orbits, min={w_min})", level="success")
            else:
                LOG.add("task7", f"W{k} CRASHED (exit {rc})", level="error")
                try:
                    log_path = Path(__file__).parent / f"log_task7_w{k}.txt"
                    with open(log_path) as f:
                        for line in f.readlines()[-3:]:
                            LOG.add("task7", f"  {line.rstrip()}", level="error")
                except:
                    pass

        # Summary line every ~15s
        now = time.time()
        if now - last_summary_time >= 15:
            last_summary_time = now
            total_done = 0
            total_todo = 0
            global_best = 81
            active = 0
            for wid in range(n_workers):
                d, t, b, _ = _parse_worker_log(wid)
                total_done += d
                total_todo += t
                if b < global_best:
                    global_best = b
                if wid in procs:
                    active += 1
            # Also count already-finished workers
            for wid in range(n_workers):
                if wid not in procs:
                    cp = CheckpointManager(f"task7_w{wid}").load()
                    if cp and cp.get("status") == "done":
                        total_done += cp.get("processed", 0)
                        w_min = cp.get("global_min", 81)
                        if w_min < global_best:
                            global_best = w_min

            elapsed = now - t0
            n_finished = n_total_workers - len(procs)
            pct = round(100 * total_done / max(1, n_orbits))
            LOG.add("task7",
                     f"{total_done}/{n_orbits} orbits ({pct}%) | min={global_best} clues | "
                     f"{active} workers | {n_finished} finished | {elapsed:.0f}s",
                     level="info")

        n_done = n_total_workers - len(procs)
        if progress_callback:
            pct = 5 + int(80 * n_done / max(1, n_total_workers))
            progress_callback({"phase": "parallel", "percent": pct,
                               "message": f"Workers {n_done}/{n_total_workers} done"})

    # Merge all worker results
    LOG.add("task7", "Merging worker results...", level="info")
    global_min = 81
    all_results = {}
    all_distribution = {}
    total_processed = 0

    for wid in range(n_workers):
        cp = CheckpointManager(f"task7_w{wid}").load()
        if not cp:
            LOG.add("task7", f"  W{wid}: no checkpoint found", level="warning")
            continue

        w_results = cp.get("results", {})
        w_min = cp.get("global_min", 81)
        w_dist = cp.get("distribution", {})
        w_proc = cp.get("processed", 0)

        all_results.update(w_results)
        total_processed += w_proc
        if w_min < global_min:
            global_min = w_min
        for k, v in w_dist.items():
            all_distribution[k] = all_distribution.get(k, 0) + v

        status = cp.get("status", "?")
        LOG.add("task7", f"  W{wid}: {w_proc} orbits, min={w_min} ({status})", level="info")

    elapsed = round(time.time() - t0, 1)

    LOG.add("task7", "=" * 60, level="success")
    LOG.add("task7", f"DONE: {total_processed} orbits in {elapsed:.0f}s ({elapsed/60:.1f} min)", level="success")
    LOG.add("task7", f"MINIMUM CLUES (Variant A): {global_min}", level="success")
    LOG.add("task7", f"Distribution: {all_distribution}", level="math")

    # Export
    try:
        export_data = {
            "description": "Minimum clue analysis for Suirodoku 9×9 (Variant A: full pairs)",
            "method": f"Parallel greedy removal + CP-SAT ({n_workers} workers), 5 shuffles per orbit",
            "orbits_analyzed": total_processed,
            "global_minimum": global_min,
            "distribution": all_distribution,
            "results": all_results,
            "elapsed": elapsed,
            "timestamp": datetime.now().isoformat(),
        }
        with open(EXPORT_DIR / "task7_min_clues_9x9.json", "w") as f:
            json.dump(export_data, f, indent=2, default=str)
        LOG.add("task7", "Export -> exports/task7_min_clues_9x9.json")
    except Exception as e:
        LOG.add("task7", f"Export error: {e}", level="error")

    result = {
        "status": "done", "method": "parallel",
        "processed": total_processed, "total": n_orbits,
        "global_min": global_min,
        "results": all_results,
        "distribution": all_distribution,
        "elapsed": elapsed,
    }
    ckpt_merged.save(result)
    return result


# =============================================================
# Task 8: Detailed Stabilizer Analysis
# =============================================================

def decompose_row_perm(row_perm):
    """Decompose a 9-element row permutation into band_perm + within-band perms."""
    band_perm = tuple(row_perm[i*3] // 3 for i in range(3))
    within = []
    for i in range(3):
        src_band = band_perm[i]
        base = src_band * 3
        within.append(tuple(row_perm[i*3 + j] - base for j in range(3)))
    return band_perm, within


def decompose_col_perm(col_perm):
    """Decompose a 9-element col permutation into stack_perm + within-stack perms."""
    stack_perm = tuple(col_perm[i*3] // 3 for i in range(3))
    within = []
    for i in range(3):
        src_stack = stack_perm[i]
        base = src_stack * 3
        within.append(tuple(col_perm[i*3 + j] - base for j in range(3)))
    return stack_perm, within


def classify_transform(row_perm, col_perm, rot):
    """Classify a structural transform into a human-readable type."""
    identity_row = tuple(range(9))
    identity_col = tuple(range(9))
    identity_3 = (0, 1, 2)

    band_perm, row_within = decompose_row_perm(row_perm)
    stack_perm, col_within = decompose_col_perm(col_perm)

    rows_trivial = (row_perm == identity_row)
    cols_trivial = (col_perm == identity_col)
    bands_trivial = (band_perm == identity_3)
    stacks_trivial = (stack_perm == identity_3)
    rows_within_trivial = all(w == identity_3 for w in row_within)
    cols_within_trivial = all(w == identity_3 for w in col_within)

    parts = []

    if rot > 0:
        parts.append(f"rot{rot*90}")

    if not bands_trivial:
        parts.append(f"bands{''.join(str(x) for x in band_perm)}")

    if not stacks_trivial:
        parts.append(f"stacks{''.join(str(x) for x in stack_perm)}")

    if not rows_within_trivial:
        for i, w in enumerate(row_within):
            if w != identity_3:
                parts.append(f"rowB{i}:{''.join(str(x) for x in w)}")

    if not cols_within_trivial:
        for i, w in enumerate(col_within):
            if w != identity_3:
                parts.append(f"colS{i}:{''.join(str(x) for x in w)}")

    if not parts:
        return "identity"

    return "+".join(parts)


def compute_detailed_stabilizer(grid):
    """Compute stabilizer with full transform details.
    Enumerates all 3,359,232 transforms.
    Returns (stab_size, list of fixing transforms)."""
    fixes = []
    for row_perm, col_perm, rot in enumerate_structural_transforms():
        transformed = apply_structural_transform(grid, row_perm, col_perm, rot)
        is_relab, digit_map, color_map = check_is_relabeling(grid, transformed)
        if is_relab:
            desc = classify_transform(row_perm, col_perm, rot)
            fixes.append({
                "row_perm": list(row_perm),
                "col_perm": list(col_perm),
                "rot": rot,
                "digit_map": digit_map,
                "color_map": color_map,
                "description": desc,
            })
    
    return len(fixes), fixes

# =============================================================
# Task 10 — SYMMETRY HUNTER (integrated)
# =============================================================
# Replaces the standalone symmetry_hunter.py.
# 4 sub-tasks, each launchable independently:
#
# Task 10a — Back-Circulant Mates
#   Trouve TOUS les mates couleur du Sudoku back-circulant.
#   C'est le Sudoku le plus symétrique connu. On énumère
#   exhaustivement toutes les couches couleur compatibles.
#   Résultat: nombre exact de mates × 9.
#
# Task 10b — Cyclic Mates
#   Même chose pour d'autres Sudoku cycliques (sw1_sb2, etc.)
#   Compare le nombre de mates entre différentes structures.
#
# Task 10c — Imposed Symmetry
#   Cherche des grilles Suirodoku avec des symétries imposées
#   directement dans le modèle CP-SAT (rot90, band_cyclic, etc.)
#   Cible: grilles à haut stabilisateur.
#
# Task 10d — Swap + Transform
#   Cherche des grilles auto-duales: échanger chiffres↔couleurs
#   puis appliquer une transformation structurelle donne la même grille.
#   C'est la symétrie la plus rare et la plus intéressante.
#
# Toutes les grilles trouvées sont automatiquement injectées dans
# le checkpoint Task 03 → Task 04 les classifie ensuite en orbites.
# =============================================================

# --- Known highly-symmetric Sudoku digit layers ---

BACK_CIRCULANT = [
    [1,2,3, 4,5,6, 7,8,9],
    [4,5,6, 7,8,9, 1,2,3],
    [7,8,9, 1,2,3, 4,5,6],
    [2,3,1, 5,6,4, 8,9,7],
    [8,9,7, 2,3,1, 5,6,4],
    [5,6,4, 8,9,7, 2,3,1],
    [9,7,8, 3,1,2, 6,4,5],
    [3,1,2, 6,4,5, 9,7,8],
    [6,4,5, 9,7,8, 3,1,2],
]

BACK_CIRCULANT_V2 = [
    [1,2,3, 4,5,6, 7,8,9],
    [4,5,6, 7,8,9, 1,2,3],
    [7,8,9, 1,2,3, 4,5,6],
    [3,1,2, 6,4,5, 9,7,8],
    [6,4,5, 9,7,8, 3,1,2],
    [9,7,8, 3,1,2, 6,4,5],
    [2,3,1, 5,6,4, 8,9,7],
    [5,6,4, 8,9,7, 2,3,1],
    [8,9,7, 2,3,1, 5,6,4],
]


def _generate_cyclic_sudoku(shift_within, shift_bands):
    """Generate a cyclic Sudoku with given shift parameters."""
    grid = []
    for band in range(3):
        for row in range(3):
            offset = (shift_bands * band + shift_within * row) % 9
            grid.append([(offset + c) % 9 + 1 for c in range(9)])
    return grid


def _build_cyclic_grids():
    """Build dict of valid cyclic Sudoku grids."""
    grids = {}
    for sw in [1, 3]:
        for sb in [1, 2, 3]:
            key = f"cyclic_sw{sw}_sb{sb}"
            g = _generate_cyclic_sudoku(sw, sb)
            valid = True
            for r in range(9):
                if len(set(g[r])) != 9:
                    valid = False
            for c in range(9):
                if len(set(g[r_][c] for r_ in range(9))) != 9:
                    valid = False
            for br in range(3):
                for bc in range(3):
                    block = [g[r_][c_] for r_ in range(br*3, (br+1)*3) for c_ in range(bc*3, (bc+1)*3)]
                    if len(set(block)) != 9:
                        valid = False
            if valid:
                grids[key] = g
    grids["back_circulant"] = BACK_CIRCULANT
    grids["back_circulant_v2"] = BACK_CIRCULANT_V2
    return grids


CYCLIC_GRIDS = _build_cyclic_grids()


def _find_all_color_mates(digit_grid, exhaustive=False, max_solutions=1000,
                          timeout=300, on_solution_found=None, sample_size=500,
                          use_rot180=True, symmetry_fn=None, _collector_ref=None,
                          partition=None, partition2_values=None):
    """Find all orthogonal color layers for a given Sudoku digit grid.

    Args:
        digit_grid: 9×9 list of ints (1-9)
        exhaustive: if True, enumerate ALL with breaking color[0][0]=0
                    then ×9 = exact total count
        max_solutions: cap (ignored if exhaustive)
        timeout: seconds
        on_solution_found: callback(n_found, elapsed_seconds) called per solution
    Returns:
        list of grids (tuples of (digit-1, color) pairs), solver status
    """
    from ortools.sat.python import cp_model

    N = 9
    B = 3

    if exhaustive:
        max_solutions = 0  # 0 = no cap, enumerate ALL
        timeout = max(timeout, 86400)  # 24h max

    model = cp_model.CpModel()
    color = [[model.NewIntVar(0, 8, f'k_{r}_{c}') for c in range(N)] for r in range(N)]

    for r in range(N):
        model.AddAllDifferent(color[r])
    for c in range(N):
        model.AddAllDifferent([color[r][c] for r in range(N)])
    for br in range(B):
        for bc in range(B):
            model.AddAllDifferent([
                color[r][c]
                for r in range(br*B, (br+1)*B)
                for c in range(bc*B, (bc+1)*B)
            ])

    pair = [[model.NewIntVar(0, 80, f'p_{r}_{c}') for c in range(N)] for r in range(N)]
    for r in range(N):
        for c in range(N):
            d = digit_grid[r][c] - 1
            model.Add(pair[r][c] == d * N + color[r][c])
    model.AddAllDifferent([pair[r][c] for r in range(N) for c in range(N)])

    # Symmetry breaking: fix color[0][0] = 0
    # raw_total = found × 9 (exact)
    model.Add(color[0][0] == 0)

    # Partition: fix color[0][1] to split work across parallel workers
    if partition is not None:
        model.Add(color[0][1] == partition)

    # Sub-partition: restrict color[0][2] to a subset of values
    if partition2_values is not None:
        from ortools.sat.python.cp_model import Domain
        model.AddLinearExpressionInDomain(
            color[0][2], Domain.FromValues(partition2_values))

    # ROT180 / symmetry: impose symmetry constraint on color layer.
    # Only possible if digit_grid itself has the corresponding symmetry.
    # If incompatible → no mates with this symmetry exist → return 0 results.
    _sym_incompatible = False
    if symmetry_fn is not None:
        tau = symmetry_fn(model, color, digit_grid, N)
        if tau is None:
            _sym_incompatible = True
            LOG.add("engine", f"Symmetry {symmetry_fn.__name__}: digit grid incompatible "
                    f"→ 0 mates with this symmetry exist", level="warning")
    elif use_rot180:
        tau = add_rot180_color_only(model, color, digit_grid, N)
        if tau is None:
            _sym_incompatible = True
            LOG.add("engine", "rot180: digit grid incompatible "
                    "→ 0 mates with rot180 symmetry exist", level="warning")

    if _sym_incompatible:
        # Return immediately: no solutions possible
        return [], 0, 0

    class SolutionCollector(cp_model.CpSolverSolutionCallback):

        def __init__(self, color_vars, dg, max_sol, on_found=None, reservoir_size=500):
            super().__init__()
            self.color_vars = color_vars
            self.dg = dg
            self.max_sol = max_sol  # 0 = no cap
            self.count = 0
            self.SAMPLE_SIZE = reservoir_size
            self.reservoir = []  # reservoir sample of SAMPLE_SIZE grids
            self.start_time = time.time()
            self.on_found = on_found
            self._rng = __import__('random').Random(42)

        def on_solution_callback(self):
            self.count += 1
            # Reservoir sampling: uniform random sample of SAMPLE_SIZE
            if self.count <= self.SAMPLE_SIZE:
                grid = self._extract_grid()
                self.reservoir.append(grid)
            else:
                j = self._rng.randint(0, self.count - 1)
                if j < self.SAMPLE_SIZE:
                    self.reservoir[j] = self._extract_grid()
            # Throttled callback: every 10K solutions, or first 100
            if self.on_found:
                if self.count <= 100 or self.count % 10000 == 0:
                    elapsed = time.time() - self.start_time
                    self.on_found(self.count, elapsed)
            if self.max_sol > 0 and self.count >= self.max_sol:
                self.StopSearch()

        def _extract_grid(self):
            return tuple(
                tuple((self.dg[r][c] - 1, self.Value(self.color_vars[r][c]))
                      for c in range(N))
                for r in range(N)
            )

    solver = cp_model.CpSolver()
    # For exhaustive enumeration, num_workers MUST be 1 (OR-Tools requirement)
    solver.parameters.num_workers = 1 if exhaustive else max(1, mp.cpu_count() - 1)
    solver.parameters.max_time_in_seconds = timeout
    solver.parameters.enumerate_all_solutions = True

    collector = SolutionCollector(color, digit_grid, max_solutions,
                                   on_found=on_solution_found, reservoir_size=sample_size)
    if _collector_ref is not None:
        _collector_ref[0] = collector
    status = solver.Solve(model, collector)

    return collector.reservoir, collector.count, status


def _find_grids_with_imposed_symmetry(symmetry_type="rot90", max_solutions=100,
                                       timeout=120, on_solution_found=None):
    """Build CP-SAT model with imposed structural symmetry.

    symmetry_type: rot90, rot180, band_cyclic, stack_cyclic,
                   band_and_stack_cyclic, row_within_band, double_cyclic
    Returns: list of grids (internal format)
    """
    from ortools.sat.python import cp_model

    N = 9
    B = 3
    model = cp_model.CpModel()
    digit = [[model.NewIntVar(0, 8, f'd_{r}_{c}') for c in range(N)] for r in range(N)]
    color = [[model.NewIntVar(0, 8, f'k_{r}_{c}') for c in range(N)] for r in range(N)]

    # Standard Suirodoku constraints
    for r in range(N):
        model.AddAllDifferent(digit[r])
        model.AddAllDifferent(color[r])
    for c in range(N):
        model.AddAllDifferent([digit[r][c] for r in range(N)])
        model.AddAllDifferent([color[r][c] for r in range(N)])
    for br in range(B):
        for bc in range(B):
            cells_d = [digit[r][c] for r in range(br*B, (br+1)*B) for c in range(bc*B, (bc+1)*B)]
            cells_k = [color[r][c] for r in range(br*B, (br+1)*B) for c in range(bc*B, (bc+1)*B)]
            model.AddAllDifferent(cells_d)
            model.AddAllDifferent(cells_k)

    pair = [[model.NewIntVar(0, 80, f'p_{r}_{c}') for c in range(N)] for r in range(N)]
    for r in range(N):
        for c in range(N):
            model.Add(pair[r][c] == digit[r][c] * N + color[r][c])
    model.AddAllDifferent([pair[r][c] for r in range(N) for c in range(N)])

    # Symmetry breaking: first row of digits = 0,1,...,8
    for c in range(N):
        model.Add(digit[0][c] == c)

    # === IMPOSE SYMMETRY ===
    if symmetry_type == "rot90":
        sigma = [model.NewIntVar(0, 8, f'sig_{i}') for i in range(N)]
        tau = [model.NewIntVar(0, 8, f'tau_{i}') for i in range(N)]
        model.AddAllDifferent(sigma)
        model.AddAllDifferent(tau)
        for r in range(N):
            for c in range(N):
                r2, c2 = c, 8 - r
                model.AddElement(digit[r][c], sigma, digit[r2][c2])
                model.AddElement(color[r][c], tau, color[r2][c2])

    elif symmetry_type == "rot180":
        sigma = [model.NewIntVar(0, 8, f'sig_{i}') for i in range(N)]
        tau = [model.NewIntVar(0, 8, f'tau_{i}') for i in range(N)]
        model.AddAllDifferent(sigma)
        model.AddAllDifferent(tau)
        for r in range(N):
            for c in range(N):
                r2, c2 = 8 - r, 8 - c
                model.AddElement(digit[r][c], sigma, digit[r2][c2])
                model.AddElement(color[r][c], tau, color[r2][c2])

    elif symmetry_type == "band_cyclic":
        sigma = [model.NewIntVar(0, 8, f'sig_{i}') for i in range(N)]
        tau = [model.NewIntVar(0, 8, f'tau_{i}') for i in range(N)]
        model.AddAllDifferent(sigma)
        model.AddAllDifferent(tau)
        for r in range(N):
            for c in range(N):
                r2 = (r + 3) % 9
                model.AddElement(digit[r][c], sigma, digit[r2][c])
                model.AddElement(color[r][c], tau, color[r2][c])

    elif symmetry_type == "stack_cyclic":
        sigma = [model.NewIntVar(0, 8, f'sig_{i}') for i in range(N)]
        tau = [model.NewIntVar(0, 8, f'tau_{i}') for i in range(N)]
        model.AddAllDifferent(sigma)
        model.AddAllDifferent(tau)
        for r in range(N):
            for c in range(N):
                c2 = (c + 3) % 9
                model.AddElement(digit[r][c], sigma, digit[r][c2])
                model.AddElement(color[r][c], tau, color[r][c2])

    elif symmetry_type == "band_and_stack_cyclic":
        sig_b = [model.NewIntVar(0, 8, f'sigb_{i}') for i in range(N)]
        tau_b = [model.NewIntVar(0, 8, f'taub_{i}') for i in range(N)]
        sig_s = [model.NewIntVar(0, 8, f'sigs_{i}') for i in range(N)]
        tau_s = [model.NewIntVar(0, 8, f'taus_{i}') for i in range(N)]
        model.AddAllDifferent(sig_b)
        model.AddAllDifferent(tau_b)
        model.AddAllDifferent(sig_s)
        model.AddAllDifferent(tau_s)
        for r in range(N):
            for c in range(N):
                r2 = (r + 3) % 9
                c2 = (c + 3) % 9
                model.AddElement(digit[r][c], sig_b, digit[r2][c])
                model.AddElement(color[r][c], tau_b, color[r2][c])
                model.AddElement(digit[r][c], sig_s, digit[r][c2])
                model.AddElement(color[r][c], tau_s, color[r][c2])

    elif symmetry_type == "row_within_band":
        sigma01 = [model.NewIntVar(0, 8, f'sig01_{i}') for i in range(N)]
        tau01 = [model.NewIntVar(0, 8, f'tau01_{i}') for i in range(N)]
        model.AddAllDifferent(sigma01)
        model.AddAllDifferent(tau01)
        for c in range(N):
            model.AddElement(digit[0][c], sigma01, digit[1][c])
            model.AddElement(digit[1][c], sigma01, digit[0][c])
            model.AddElement(color[0][c], tau01, color[1][c])
            model.AddElement(color[1][c], tau01, color[0][c])
            for r in [2,3,4,5,6,7,8]:
                model.AddElement(digit[r][c], sigma01, digit[r][c])
                model.AddElement(color[r][c], tau01, color[r][c])

        sigma34 = [model.NewIntVar(0, 8, f'sig34_{i}') for i in range(N)]
        tau34 = [model.NewIntVar(0, 8, f'tau34_{i}') for i in range(N)]
        model.AddAllDifferent(sigma34)
        model.AddAllDifferent(tau34)
        for c in range(N):
            model.AddElement(digit[3][c], sigma34, digit[4][c])
            model.AddElement(digit[4][c], sigma34, digit[3][c])
            model.AddElement(color[3][c], tau34, color[4][c])
            model.AddElement(color[4][c], tau34, color[3][c])
            for r in [0,1,2,5,6,7,8]:
                model.AddElement(digit[r][c], sigma34, digit[r][c])
                model.AddElement(color[r][c], tau34, color[r][c])

        sigma67 = [model.NewIntVar(0, 8, f'sig67_{i}') for i in range(N)]
        tau67 = [model.NewIntVar(0, 8, f'tau67_{i}') for i in range(N)]
        model.AddAllDifferent(sigma67)
        model.AddAllDifferent(tau67)
        for c in range(N):
            model.AddElement(digit[6][c], sigma67, digit[7][c])
            model.AddElement(digit[7][c], sigma67, digit[6][c])
            model.AddElement(color[6][c], tau67, color[7][c])
            model.AddElement(color[7][c], tau67, color[6][c])
            for r in [0,1,2,3,4,5,8]:
                model.AddElement(digit[r][c], sigma67, digit[r][c])
                model.AddElement(color[r][c], tau67, color[r][c])

    elif symmetry_type == "double_cyclic":
        shift3_table = [(i + 3) % 9 for i in range(9)]
        shift6_table = [(i + 6) % 9 for i in range(9)]
        for band in range(3):
            r0, r1, r2 = band*3, band*3+1, band*3+2
            for c in range(N):
                model.AddElement(digit[r0][c], shift3_table, digit[r1][c])
                model.AddElement(digit[r0][c], shift6_table, digit[r2][c])
                model.AddElement(color[r0][c], shift3_table, color[r1][c])
                model.AddElement(color[r0][c], shift6_table, color[r2][c])

    # Solve
    class SolutionCollector(cp_model.CpSolverSolutionCallback):
        def __init__(self, d, k, max_sol, on_found=None):
            super().__init__()
            self.d = d
            self.k = k
            self.max_sol = max_sol
            self.solutions = []
            self.start_time = time.time()
            self.on_found = on_found

        def on_solution_callback(self):
            grid = tuple(
                tuple((self.Value(self.d[r][c]), self.Value(self.k[r][c]))
                      for c in range(N))
                for r in range(N)
            )
            self.solutions.append(grid)
            n = len(self.solutions)
            if self.on_found:
                self.on_found(n, time.time() - self.start_time)
            if n >= self.max_sol:
                self.StopSearch()

    solver = cp_model.CpSolver()
    solver.parameters.num_workers = max(1, mp.cpu_count() - 1)
    solver.parameters.max_time_in_seconds = timeout
    solver.parameters.enumerate_all_solutions = True

    collector = SolutionCollector(digit, color, max_solutions, on_found=on_solution_found)
    solver.Solve(model, collector)
    return collector.solutions


def _find_grids_with_swap_symmetry(transform_type="rot180", max_solutions=100,
                                    timeout=180, on_solution_found=None):
    """Find Suirodoku grids where swap(G) ∘ transform = relabeling of G.

    This is "mixed" symmetry: structural transform + digit↔color exchange.

    transform_type: rot180, rot90, band_cyclic, stack_cyclic, transpose,
                    band_stack_cyclic
    Returns: (grids, swap_infos) where swap_infos has sigma/tau permutations
    """
    from ortools.sat.python import cp_model

    N = 9
    B = 3
    model = cp_model.CpModel()
    digit = [[model.NewIntVar(0, 8, f'd_{r}_{c}') for c in range(N)] for r in range(N)]
    color = [[model.NewIntVar(0, 8, f'k_{r}_{c}') for c in range(N)] for r in range(N)]

    # Standard Suirodoku constraints
    for r in range(N):
        model.AddAllDifferent(digit[r])
        model.AddAllDifferent(color[r])
    for c in range(N):
        model.AddAllDifferent([digit[r][c] for r in range(N)])
        model.AddAllDifferent([color[r][c] for r in range(N)])
    for br in range(B):
        for bc in range(B):
            cells_d = [digit[r][c] for r in range(br*B, (br+1)*B) for c in range(bc*B, (bc+1)*B)]
            cells_k = [color[r][c] for r in range(br*B, (br+1)*B) for c in range(bc*B, (bc+1)*B)]
            model.AddAllDifferent(cells_d)
            model.AddAllDifferent(cells_k)

    pair = [[model.NewIntVar(0, 80, f'p_{r}_{c}') for c in range(N)] for r in range(N)]
    for r in range(N):
        for c in range(N):
            model.Add(pair[r][c] == digit[r][c] * N + color[r][c])
    model.AddAllDifferent([pair[r][c] for r in range(N) for c in range(N)])

    # Symmetry breaking
    for c in range(N):
        model.Add(digit[0][c] == c)

    # Swap relabeling permutations
    sigma = [model.NewIntVar(0, 8, f'sig_{i}') for i in range(N)]
    tau = [model.NewIntVar(0, 8, f'tau_{i}') for i in range(N)]
    model.AddAllDifferent(sigma)
    model.AddAllDifferent(tau)

    def get_target(r, c, ttype):
        if ttype == "rot180":
            return 8 - r, 8 - c
        elif ttype == "rot90":
            return c, 8 - r
        elif ttype == "band_cyclic":
            return (r + 3) % 9, c
        elif ttype == "stack_cyclic":
            return r, (c + 3) % 9
        elif ttype == "transpose":
            return c, r
        elif ttype == "band_stack_cyclic":
            return (r + 3) % 9, (c + 3) % 9
        else:
            raise ValueError(f"Unknown transform: {ttype}")

    # SWAP + TRANSFORM constraint:
    # σ(color[T(r,c)]) = digit[r][c]
    # τ(digit[T(r,c)]) = color[r][c]
    for r in range(N):
        for c in range(N):
            r2, c2 = get_target(r, c, transform_type)
            model.AddElement(color[r2][c2], sigma, digit[r][c])
            model.AddElement(digit[r2][c2], tau, color[r][c])

    class SolutionCollector(cp_model.CpSolverSolutionCallback):
        def __init__(self, d, k, s, t, max_sol, on_found=None):
            super().__init__()
            self.d = d
            self.k = k
            self.s = s
            self.t = t
            self.max_sol = max_sol
            self.solutions = []
            self.start_time = time.time()
            self.on_found = on_found

        def on_solution_callback(self):
            grid = tuple(
                tuple((self.Value(self.d[r][c]), self.Value(self.k[r][c]))
                      for c in range(N))
                for r in range(N)
            )
            sig = tuple(self.Value(self.s[i]) for i in range(N))
            tau_ = tuple(self.Value(self.t[i]) for i in range(N))
            self.solutions.append((grid, sig, tau_))
            n = len(self.solutions)
            if self.on_found:
                self.on_found(n, time.time() - self.start_time)
            if n >= self.max_sol:
                self.StopSearch()

    solver = cp_model.CpSolver()
    solver.parameters.num_workers = max(1, mp.cpu_count() - 1)
    solver.parameters.max_time_in_seconds = timeout
    solver.parameters.enumerate_all_solutions = True

    collector = SolutionCollector(digit, color, sigma, tau, max_solutions, on_found=on_solution_found)
    solver.Solve(model, collector)

    grids = [sol[0] for sol in collector.solutions]
    swap_infos = [{"sigma": sol[1], "tau": sol[2]} for sol in collector.solutions]
    return grids, swap_infos


def _inject_grids_into_task3(new_grids, source_tag, log_task="task10"):
    """Inject hunter-found grids into the Task 03 checkpoint.

    This way Task 04 (orbit classification) automatically picks them up.
    Returns: (n_injected, n_skipped)
    """
    ckpt3 = CheckpointManager("task3_search_grids")
    saved3 = ckpt3.load() or {"status": "done", "found_grids": [], "known_hashes": [], "attempts": 0}

    existing_hashes = set(saved3.get("known_hashes", []))
    # Also check grid hashes from found_grids
    for g in saved3.get("found_grids", []):
        h = g.get("grid_hash", "")
        if h:
            existing_hashes.add(h)

    injected = 0
    skipped = 0

    for grid in new_grids:
        h = hashlib.md5(str(grid).encode()).hexdigest()
        if h in existing_hashes:
            skipped += 1
            continue

        # Convert to display format for Task 03
        grid_display = [
            [{"d": cell[0] + 1, "c": COLORS_9[cell[1]]} for cell in row]
            for row in grid
        ]

        saved3["found_grids"].append({
            "grid_hash": h,
            "grid": grid_display,
            "found_at_attempt": -1,
            "solve_time": 0,
            "seed": f"hunter:{source_tag}",
            "verification": {"all_valid": True, "source": source_tag},
        })
        existing_hashes.add(h)
        injected += 1

    saved3["known_hashes"] = list(existing_hashes)
    ckpt3.save(saved3)

    LOG.add(log_task, f"Injected {injected} grids into Task 03 (skipped {skipped} dupes)", level="success")
    return injected, skipped


def _canon_worker_single(grid):
    """Worker: canonicalize one grid via C, return hash."""
    h, _, _ = fast_canonicalize_and_stab(grid)
    return h

def _canon_stab_worker(grid):
    """Worker: canonicalize + stab via C, return (hash, stab)."""
    try:
        h, _, stab = fast_canonicalize_and_stab(grid)
        return h, stab
    except Exception:
        return None, 0


# =============================================================
# Task 10a — Back-Circulant Mates: PURE COUNT (exhaustive)
# =============================================================

def task10a_back_circulant_mates(progress_callback=None, stop_event=None):
    """Task 10a — Count ALL color mates for the back-circulant Sudoku.

    PHASE UNIQUE: CP-SAT exhaustive enumeration.
    - Breaking color[0][0]=0, résultat × 9 = total exact
    - Reservoir sample de 100,000 grilles en RAM (pour Task 10a.b)
    - Aucune canonicalization, aucun stabilizer ici

    Résultat: le nombre exact de mates. L'analyse est dans Task 10a.b.
    """
    ckpt = CheckpointManager("task10a_back_circulant")
    saved = ckpt.load()
    if saved and saved.get("status") == "done":
        return saved

    LOG.add("task10a", "=" * 60, level="success")
    LOG.add("task10a", "TASK 10a — COUNT ALL MATES FOR BACK-CIRCULANT", level="success")
    LOG.add("task10a", "=" * 60, level="success")
    LOG.add("task10a", "Pure counting mode — sample of 100,000 grids kept for 10a.b", level="info")
    LOG.add("task10a", "Breaking: color[0][0]=0 → result × 9 = exact total", level="math")

    start = time.time()

    if progress_callback:
        progress_callback({"phase": "solving", "percent": 5, "message": "CP-SAT solving (exhaustive)..."})

    def _on_mate_found(n, elapsed):
        # Called every 10K solutions (throttled in SolutionCollector)
        rate = n / max(1, elapsed)
        LOG.add("task10a", f"  {n:,} mates found ({elapsed:.0f}s, {rate:.0f}/s)", level="info")
        if progress_callback:
            progress_callback({
                "phase": "solving",
                "percent": min(95, 5 + int(elapsed / 120)),
                "message": f"Counting... {n:,} mates ({elapsed:.0f}s, {rate:.0f}/s)",
                "found": n,
            })
        # Periodic lightweight checkpoint (just count, no grids)
        if n % 100000 == 0:
            ckpt.save({"status": "running", "phase": "solving",
                        "n_breaking_so_far": n, "elapsed_so_far": elapsed})

    mates_sample, n_breaking, status = _find_all_color_mates(
        BACK_CIRCULANT, exhaustive=True, timeout=86400,
        on_solution_found=_on_mate_found, sample_size=100000)

    from ortools.sat.python import cp_model as cpm
    elapsed = round(time.time() - start, 1)
    is_exact = status in (cpm.OPTIMAL, cpm.FEASIBLE) and elapsed < 86000
    n_total = n_breaking * 9

    LOG.add("task10a", f"RESULT: {n_breaking:,} mates (breaking) × 9 = {n_total:,} total", level="success")
    LOG.add("task10a", f"Status: {'EXACT (optimal)' if is_exact else 'INCOMPLETE (lower bound)'}", level="success" if is_exact else "warning")
    LOG.add("task10a", f"Sample: {len(mates_sample)} grids kept (reservoir sampling)", level="info")
    LOG.add("task10a", f"Time: {elapsed}s ({elapsed/60:.1f} min)", level="info")

    # Save result + sample for Task 10a.b
    result = {
        "status": "done",
        "source": "back_circulant",
        "n_mates_breaking": n_breaking,
        "n_mates_total": n_total,
        "is_exact": is_exact,
        "n_sample": len(mates_sample),
        "sample": [_serialize_grid(m) for m in mates_sample],
        "elapsed": elapsed,
    }
    ckpt.save(result)

    # Quick JSON export (count only, no full grids)
    export_data = {
        "description": "Exact count of color mates for the back-circulant Sudoku",
        "method": "CP-SAT exhaustive, breaking color[0][0]=0, ×9 = exact total",
        "source_sudoku": "back_circulant",
        "n_mates_breaking": n_breaking,
        "n_mates_total": n_total,
        "is_exact": is_exact,
        "n_sample_stored": len(mates_sample),
        "elapsed_seconds": elapsed,
        "timestamp": datetime.now().isoformat(),
    }
    with open(EXPORT_DIR / "task10a_back_circulant.json", "w") as f:
        json.dump(export_data, f, indent=2, default=str)
    LOG.add("task10a", "Export -> exports/task10a_back_circulant.json", level="info")

    if progress_callback:
        progress_callback({"percent": 100, "phase": "done"})
    return result


# =============================================================
# Task 10a.b — Back-Circulant: Analyze Sample
# =============================================================

def task10ab_analyze_sample(progress_callback=None, stop_event=None):
    """Task 10a.b — Canonicalize all grids from Task 10a sample.

    Reads 100,000 grids from Task 10a checkpoint, then:
    1. Canon + stab via C (séquentiel, ~0.15s/grid)
    2. Swap check for self-duality on unique orbits
    3. Export ALL unique orbits to JSON
    """
    ckpt_ab = CheckpointManager("task10ab_analyze")
    saved_ab = ckpt_ab.load()
    if saved_ab and saved_ab.get("status") == "done":
        return saved_ab

    if not canon_lib_available():
        msg = "canon.dll/.so introuvable."
        LOG.add("task10ab", msg, level="error")
        if progress_callback:
            progress_callback({"percent": 0, "message": msg})
        return {"status": "error", "message": msg}

    # --- Read Task 10a ---
    ckpt_10a = CheckpointManager("task10a_back_circulant")
    saved_10a = ckpt_10a.load()
    if not saved_10a or saved_10a.get("status") != "done":
        msg = "Task 10a must be completed first."
        LOG.add("task10ab", msg, level="error")
        if progress_callback:
            progress_callback({"percent": 0, "message": msg})
        return {"status": "error", "message": msg}

    n_breaking = saved_10a["n_mates_breaking"]
    n_total = saved_10a["n_mates_total"]
    is_exact = saved_10a["is_exact"]
    mates_sample = [_deserialize_grid(g) for g in saved_10a["sample"]]
    n_sample = len(mates_sample)

    LOG.add("task10ab", "=" * 60, level="success")
    LOG.add("task10ab", "TASK 10a.b — CANONICALIZE SAMPLE", level="success")
    LOG.add("task10ab", "=" * 60, level="success")
    LOG.add("task10ab", f"Source: {n_total:,} total mates, sample={n_sample:,}", level="info")

    t0 = time.time()

    # --- Resume support ---
    start_idx = 0
    orbit_counts = {}
    orbit_stabs = {}
    orbit_grids = {}
    if saved_ab and saved_ab.get("status") == "running":
        start_idx = saved_ab.get("processed", 0)
        orbit_counts = saved_ab.get("orbit_counts", {})
        orbit_stabs = saved_ab.get("orbit_stabs", {})
        # Grids can't be saved in checkpoint (too big), re-collect them
        LOG.add("task10ab", f"Resuming from grid {start_idx:,}", level="info")

    # === PHASE 1: CANON + STAB ===
    LOG.add("task10ab", f"[1/2] Canon + stab ({n_sample:,} grids, séquentiel)...", level="info")
    if progress_callback:
        progress_callback({"phase": "canonicalize", "percent": 1,
                           "message": f"Canon 0/{n_sample:,}..."})

    errors = 0
    last_save = time.time()

    for i in range(start_idx, n_sample):
        if stop_event and stop_event.is_set():
            LOG.add("task10ab", f"STOPPED at {i:,}/{n_sample:,}", level="warning")
            ckpt_ab.save({
                "status": "running", "processed": i,
                "orbit_counts": orbit_counts, "orbit_stabs": orbit_stabs,
            })
            return {"status": "paused"}

        try:
            h, _, stab = fast_canonicalize_and_stab(mates_sample[i])
        except Exception as e:
            errors += 1
            if errors <= 5:
                LOG.add("task10ab", f"Canon error #{errors}: {e}", level="error")
            continue

        if h not in orbit_counts:
            orbit_counts[h] = 0
            orbit_stabs[h] = stab
            orbit_grids[h] = mates_sample[i]
        orbit_counts[h] += 1

        done = i + 1
        if done % 500 == 0 or done == n_sample:
            elapsed = time.time() - t0
            rate = (done - start_idx) / max(0.1, elapsed)
            eta = (n_sample - done) / max(1, rate)
            n_orb = len(orbit_counts)
            top_s = max(orbit_stabs.values()) if orbit_stabs else 0
            LOG.add("task10ab",
                     f"  {done:,}/{n_sample:,} — {n_orb:,} orbits, top={top_s} "
                     f"({rate:.1f}/s, ETA {eta/60:.0f}min)")
            if progress_callback:
                pct = 1 + round(80 * done / n_sample)
                progress_callback({"phase": "canonicalize", "percent": pct,
                                   "message": f"Canon {done:,}/{n_sample:,} — {n_orb:,} orbits"})

        # Periodic checkpoint (every 60s)
        now = time.time()
        if now - last_save > 60:
            last_save = now
            ckpt_ab.save({
                "status": "running", "processed": done,
                "orbit_counts": orbit_counts, "orbit_stabs": orbit_stabs,
            })

    n_orbits = len(orbit_counts)
    LOG.add("task10ab", f"[1/2] Done: {n_orbits:,} orbits ({errors} errors)", level="success")

    # === PHASE 2: SWAP CHECK ===
    LOG.add("task10ab", f"[2/2] Swap check ({n_orbits:,} orbits)...", level="info")
    if progress_callback:
        progress_callback({"phase": "swap", "percent": 82,
                           "message": f"Swap 0/{n_orbits:,}"})

    orbit_self_dual = {}
    for idx, h in enumerate(orbit_grids):
        try:
            swapped = swap_grid(orbit_grids[h])
            swap_h, _, _ = fast_canonicalize_and_stab(swapped)
            orbit_self_dual[h] = (h == swap_h)
        except:
            orbit_self_dual[h] = False

        if (idx + 1) % 100 == 0 or (idx + 1) == n_orbits:
            if progress_callback:
                pct = 82 + round(15 * (idx + 1) / max(1, n_orbits))
                progress_callback({"phase": "swap", "percent": pct,
                                   "message": f"Swap {idx+1:,}/{n_orbits:,}"})

    self_dual_count = sum(1 for v in orbit_self_dual.values() if v)
    LOG.add("task10ab", f"[2/2] Self-dual: {self_dual_count}/{n_orbits:,}", level="math")

    # === RESULTS ===
    elapsed = round(time.time() - t0, 1)

    stab_dist_struct = {}
    stab_dist_total = {}
    for h in orbit_stabs:
        s = orbit_stabs[h]
        sd = orbit_self_dual.get(h, False)
        total = s * (2 if sd else 1)
        stab_dist_struct[s] = stab_dist_struct.get(s, 0) + 1
        stab_dist_total[total] = stab_dist_total.get(total, 0) + 1

    sorted_hashes = sorted(orbit_counts.keys(),
                           key=lambda h: -(orbit_stabs[h] * (2 if orbit_self_dual.get(h) else 1)))

    top_stab = 0
    if sorted_hashes:
        h0 = sorted_hashes[0]
        top_stab = orbit_stabs[h0] * (2 if orbit_self_dual.get(h0) else 1)

    # Log
    LOG.add("task10ab", "=" * 60, level="success")
    LOG.add("task10ab", f"DONE in {elapsed:.0f}s ({elapsed/60:.1f} min)", level="success")
    LOG.add("task10ab", f"{n_sample:,} grids → {n_orbits:,} orbits", level="success")
    LOG.add("task10ab", f"Top |Stab| total = {top_stab}", level="success")
    LOG.add("task10ab", f"Self-dual: {self_dual_count}/{n_orbits:,}", level="math")

    LOG.add("task10ab", "Stab distribution (structural):", level="math")
    for s in sorted(stab_dist_struct.keys(), reverse=True):
        LOG.add("task10ab", f"  |Stab|={s}: {stab_dist_struct[s]} orbits", level="info")

    LOG.add("task10ab", "Top 20 orbits:", level="math")
    for h in sorted_hashes[:20]:
        s = orbit_stabs[h]
        sd = orbit_self_dual.get(h, False)
        total = s * (2 if sd else 1)
        LOG.add("task10ab",
                f"  {h[:16]}  stab={s}  {'SD' if sd else '  '}  total={total}  x{orbit_counts[h]}",
                level="success" if total >= 100 else "info")

    # === EXPORT JSON ===
    export_orbits = []
    for h in sorted_hashes:
        s = orbit_stabs[h]
        sd = orbit_self_dual.get(h, False)
        total = s * (2 if sd else 1)
        entry = {
            "canon_hash": h[:16],
            "stab_structural": s,
            "self_dual": sd,
            "stab_total": total,
            "count_in_sample": orbit_counts[h],
            "orbit_size_structural": TOTAL_STRUCTURAL // s,
        }
        if h in orbit_grids:
            entry["grid"] = grid_to_display(orbit_grids[h])
        export_orbits.append(entry)

    export_data = {
        "description": "All orbits from back-circulant 100k sample",
        "n_mates_total": n_total,
        "n_mates_breaking": n_breaking,
        "is_exact": is_exact,
        "n_sample": n_sample,
        "n_orbits": n_orbits,
        "self_dual_count": self_dual_count,
        "top_stab": top_stab,
        "stab_distribution_structural": {str(k): v for k, v in sorted(stab_dist_struct.items(), reverse=True)},
        "stab_distribution_total": {str(k): v for k, v in sorted(stab_dist_total.items(), reverse=True)},
        "orbits": export_orbits,
        "elapsed_seconds": elapsed,
        "timestamp": datetime.now().isoformat(),
    }

    try:
        export_path = EXPORT_DIR / "task10ab_analysis.json"
        export_path.parent.mkdir(parents=True, exist_ok=True)
        with open(export_path, "w") as f:
            json.dump(export_data, f, indent=2, default=str)
        LOG.add("task10ab", f"Export → {export_path}", level="info")
    except Exception as e:
        LOG.add("task10ab", f"Export error: {e}", level="error")

    # Checkpoint (sans grids pour garder petit)
    result = {
        "status": "done",
        "source": "back_circulant",
        "n_mates_total": n_total,
        "n_sample": n_sample,
        "n_orbits": n_orbits,
        "self_dual_count": self_dual_count,
        "top_stab": top_stab,
        "stab_distribution": {str(k): v for k, v in sorted(stab_dist_total.items(), reverse=True)},
        "elapsed": elapsed,
    }
    ckpt_ab.save(result)

    if progress_callback:
        progress_callback({"percent": 100, "phase": "done",
                           "message": f"Done: {n_orbits:,} orbits, top={top_stab}"})
    LOG.add("task10ab", f"DONE — {n_orbits:,} orbits, top |Stab|={top_stab} ({elapsed:.0f}s)", level="success")
    return result


# =============================================================
# Task 10b — Cyclic Mates (other cyclic Sudoku)
# =============================================================

def task10b_cyclic_mates(progress_callback=None, stop_event=None):
    """Task 10b — Color mates for other cyclic Sudoku grids.

    On teste plusieurs Sudoku cycliques (sw1_sb2, sw3_sb2, etc.)
    et on compte combien de mates couleur chacun admet.

    Compare les résultats entre les différentes structures cycliques.
    Les grilles à haut stabilisateur (≥36) sont injectées dans Task 03.
    """
    ckpt = CheckpointManager("task10b_cyclic_mates")
    saved = ckpt.load()
    if saved and saved.get("status") == "done":
        return saved

    # Resume support
    results_per_grid = {}
    total_injected = 0
    start_gi = 0
    if saved and saved.get("status") == "running":
        results_per_grid = saved.get("results_per_grid", {})
        total_injected = saved.get("total_injected", 0)
        start_gi = saved.get("completed_grids", 0)

    LOG.add("task10b", "=" * 60, level="success")
    LOG.add("task10b", "TASK 10b — COLOR MATES FOR CYCLIC SUDOKU", level="success")
    LOG.add("task10b", "=" * 60, level="success")
    LOG.add("task10b", f"Testing {len(CYCLIC_GRIDS)} cyclic grids (resuming from #{start_gi})", level="info")

    if progress_callback:
        progress_callback({"phase": "starting", "percent": 2, "message": f"Testing {len(CYCLIC_GRIDS)} cyclic grids..."})

    start = time.time()
    n_workers = max(1, mp.cpu_count() - 1)

    grid_names = list(CYCLIC_GRIDS.keys())
    for gi, (name, dgrid) in enumerate(CYCLIC_GRIDS.items()):
        if stop_event and stop_event.is_set():
            break
        if gi < start_gi:
            continue

        if name == "back_circulant":
            LOG.add("task10b", f"Skipping {name} (done in Task 10a)", level="info")
            continue

        LOG.add("task10b", f"\n--- {name} ({gi+1}/{len(CYCLIC_GRIDS)}) ---", level="info")

        def _on_mate_found_b(n, elapsed, _name=name):
            # Called every 10K (throttled by collector)
            LOG.add("task10b", f"  [{_name}] {n:,} mates ({elapsed:.0f}s)")
            if progress_callback:
                base_pct = round(100 * gi / len(CYCLIC_GRIDS))
                progress_callback({"percent": base_pct, "message": f"{_name}: {n:,} mates ({elapsed:.0f}s)"})

        mates_sample, n_breaking, status = _find_all_color_mates(dgrid, exhaustive=True, timeout=3600,
                                               on_solution_found=_on_mate_found_b)
        n_total = n_breaking * 9
        is_exact = (status == 4)

        LOG.add("task10b", f"  {n_breaking:,} mates (breaking) × 9 = {n_total:,} total (sample: {len(mates_sample)})", level="success")

        grid_result = {
            "n_mates_breaking": n_breaking,
            "n_mates_total": n_total,
            "is_exact": is_exact,
        }

        if mates_sample:
            sample = mates_sample[:min(200, len(mates_sample))]
            base_pct = round(100 * gi / len(CYCLIC_GRIDS))
            if progress_callback:
                progress_callback({"percent": base_pct, "message": f"{name}: canonicalizing {len(sample)} grids..."})

            with mp.Pool(n_workers) as pool:
                c_hashes = list(pool.imap(_canon_worker_single, sample))

            if progress_callback:
                progress_callback({"percent": base_pct, "message": f"{name}: stabilizers 0/{len(sample)}..."})

            stab_list = []
            with mp.Pool(n_workers) as pool:
                for idx, stab_size in pool.imap(_stabilizer_worker, [(i, sample[i]) for i in range(len(sample))]):
                    stab_list.append(stab_size)
                    if len(stab_list) % 5 == 0 and progress_callback:
                        progress_callback({"percent": base_pct, "message": f"{name}: stabilizers {len(stab_list)}/{len(sample)} — last |Stab|={stab_size}"})

            stab_dist = {}
            for s in stab_list:
                stab_dist[s] = stab_dist.get(s, 0) + 1

            LOG.add("task10b", f"  Stab distribution (sample of {len(sample)}):", level="math")
            for s in sorted(stab_dist.keys(), reverse=True):
                LOG.add("task10b", f"    |Stab|={s}: {stab_dist[s]}", level="info")

            high_stab = [sample[i] for i in range(len(sample)) if stab_list[i] >= 36]
            if high_stab:
                LOG.add("task10b", f"  Found {len(high_stab)} high-stab grids (|Stab|≥36) — kept separate", level="success")

            grid_result["stab_distribution"] = {str(k): v for k, v in sorted(stab_dist.items(), reverse=True)}
            grid_result["n_high_stab"] = len(high_stab)

        results_per_grid[name] = grid_result

        # Checkpoint after each grid
        ckpt.save({"status": "running", "completed_grids": gi + 1,
                    "results_per_grid": results_per_grid,
                    "total_injected": total_injected})

        if progress_callback:
            progress_callback({"percent": round(100 * (gi+1) / len(CYCLIC_GRIDS)), "message": f"Done: {name}"})

    elapsed = round(time.time() - start, 1)

    # JSON Export
    export_data = {
        "description": "Color mates for cyclic Sudoku grids",
        "method": "CP-SAT exhaustive enumeration per cyclic grid, breaking color[0][0]=0",
        "grids_tested": list(results_per_grid.keys()),
        "results_per_grid": results_per_grid,
        "total_injected": total_injected,
        "elapsed_seconds": elapsed,
        "timestamp": datetime.now().isoformat(),
    }
    with open(EXPORT_DIR / "task10b_cyclic_mates.json", "w") as f:
        json.dump(export_data, f, indent=2, default=str)
    LOG.add("task10b", "Export -> exports/task10b_cyclic_mates.json", level="info")

    result = {
        "status": "done",
        "results_per_grid": results_per_grid,
        "total_injected": total_injected,
        "elapsed": elapsed,
    }
    ckpt.save(result)

    LOG.add("task10b", f"DONE — {len(results_per_grid)} grids analyzed, {total_injected} injected", level="success")
    return result


# =============================================================
# Task 10c — Imposed Symmetry Search
# =============================================================

def task10c_unbiased_mates(progress_callback=None, stop_event=None):
    """Task 10c — 21M mates without r180 bias.

    CP-SAT enumeration sans biais r180, cap 21M breaking (= 189M total).
    Reservoir sample de 100,000 grilles, sauvé toutes les 5M pour sécurité.
    """
    ckpt = CheckpointManager("task10c_unbiased")
    saved = ckpt.load()
    if saved and saved.get("status") == "done":
        return saved

    MAX_SOL = 21_000_000

    LOG.add("task10c", "=" * 60, level="success")
    LOG.add("task10c", "TASK 10c — 21M MATES (NO r180 BIAS)", level="success")
    LOG.add("task10c", "=" * 60, level="success")
    LOG.add("task10c", "No r180 — 21M solutions, reservoir 100k", level="info")
    LOG.add("task10c", "Breaking: color[0][0]=0, count x9 = total enumerated", level="math")

    start = time.time()

    if progress_callback:
        progress_callback({"phase": "solving", "percent": 5, "message": "CP-SAT solving (no r180)..."})

    # Shared ref so callback can access reservoir for periodic save
    collector_ref = [None]

    def _on_mate_found(n, elapsed):
        rate = n / max(1, elapsed)
        pct_done = min(95, 5 + int(90 * n / MAX_SOL))
        eta_s = (MAX_SOL - n) / max(1, rate)
        if n <= 100 or n % 1_000_000 == 0:
            LOG.add("task10c", f"  {n:,} mates ({elapsed:.0f}s, {rate:.0f}/s, ETA {eta_s/60:.0f}min)", level="info")
        if progress_callback:
            progress_callback({
                "phase": "solving",
                "percent": pct_done,
                "message": f"{n:,}/{MAX_SOL//1_000_000}M ({rate:.0f}/s, ETA {eta_s/60:.0f}min)",
                "found": n,
            })
        # Save checkpoint WITH reservoir every 5M for safety
        if n % 5_000_000 == 0:
            coll = collector_ref[0]
            if coll is not None:
                LOG.add("task10c", f"  ** Saving reservoir ({len(coll.reservoir)} grids) at {n:,} **", level="info")
                ckpt.save({"status": "running", "phase": "solving",
                            "n_breaking_so_far": n, "elapsed_so_far": elapsed,
                            "n_sample": len(coll.reservoir),
                            "sample": [_serialize_grid(m) for m in coll.reservoir]})
            else:
                ckpt.save({"status": "running", "phase": "solving",
                            "n_breaking_so_far": n, "elapsed_so_far": elapsed})

    mates_sample, n_breaking, status = _find_all_color_mates(
        BACK_CIRCULANT, exhaustive=False, max_solutions=MAX_SOL, timeout=86400,
        on_solution_found=_on_mate_found, sample_size=100000,
        use_rot180=False, _collector_ref=collector_ref)

    from ortools.sat.python import cp_model as cpm
    elapsed = round(time.time() - start, 1)
    is_exact = (status == cpm.OPTIMAL)
    n_total = n_breaking * 9

    LOG.add("task10c", f"RESULT: {n_breaking:,} mates (breaking) x 9 = {n_total:,} enumerated", level="success")
    LOG.add("task10c", f"Exact total: {'YES (solver proved optimal)' if is_exact else 'NO (cap reached, more exist)'}", level="success" if is_exact else "warning")
    LOG.add("task10c", f"Sample: {len(mates_sample)} grids kept", level="info")
    LOG.add("task10c", f"Time: {elapsed}s ({elapsed/60:.1f} min)", level="info")

    result = {
        "status": "done",
        "source": "back_circulant",
        "bias": "none",
        "n_mates_breaking": n_breaking,
        "n_mates_total": n_total,
        "is_exact": is_exact,
        "n_sample": len(mates_sample),
        "sample": [_serialize_grid(m) for m in mates_sample],
        "elapsed": elapsed,
    }
    ckpt.save(result)

    export_data = {
        "description": "21M color mates for back-circulant (no r180 bias)",
        "method": "CP-SAT 21M max, color[0][0]=0 x9, NO r180",
        "source_sudoku": "back_circulant",
        "n_mates_breaking": n_breaking,
        "n_mates_total": n_total,
        "is_exact": is_exact,
        "n_sample_stored": len(mates_sample),
        "elapsed_seconds": elapsed,
        "timestamp": datetime.now().isoformat(),
    }
    try:
        export_path = EXPORT_DIR / "task10c_unbiased.json"
        export_path.parent.mkdir(parents=True, exist_ok=True)
        with open(export_path, "w") as f:
            json.dump(export_data, f, indent=2, default=str)
        LOG.add("task10c", f"Export -> {export_path}", level="info")
    except Exception as e:
        LOG.add("task10c", f"Export error: {e}", level="error")

    if progress_callback:
        progress_callback({"percent": 100, "phase": "done",
                           "message": f"Done: {n_total:,} mates"})
    return result


def task10d_unbiased_analysis(progress_callback=None, stop_event=None):
    """Task 10d — Canonicalize all grids from Task 10c (unbiased) sample."""
    ckpt_d = CheckpointManager("task10d_unbiased")
    saved_d = ckpt_d.load()
    if saved_d and saved_d.get("status") == "done":
        return saved_d

    if not canon_lib_available():
        msg = "canon.dll/.so introuvable."
        LOG.add("task10d", msg, level="error")
        if progress_callback:
            progress_callback({"percent": 0, "message": msg})
        return {"status": "error", "message": msg}

    ckpt_10c = CheckpointManager("task10c_unbiased")
    saved_10c = ckpt_10c.load()
    if not saved_10c or saved_10c.get("status") != "done":
        msg = "Task 10c must be completed first."
        LOG.add("task10d", msg, level="error")
        if progress_callback:
            progress_callback({"percent": 0, "message": msg})
        return {"status": "error", "message": msg}

    n_breaking = saved_10c["n_mates_breaking"]
    n_total = saved_10c["n_mates_total"]
    is_exact = saved_10c["is_exact"]
    mates_sample = [_deserialize_grid(g) for g in saved_10c["sample"]]
    n_sample = len(mates_sample)

    LOG.add("task10d", "=" * 60, level="success")
    LOG.add("task10d", "TASK 10d — CANONICALIZE UNBIASED SAMPLE", level="success")
    LOG.add("task10d", "=" * 60, level="success")
    LOG.add("task10d", f"Source: {n_total:,} total mates (unbiased), sample={n_sample:,}", level="info")

    t0 = time.time()

    start_idx = 0
    orbit_counts = {}
    orbit_stabs = {}
    orbit_grids = {}
    if saved_d and saved_d.get("status") == "running":
        start_idx = saved_d.get("processed", 0)
        orbit_counts = saved_d.get("orbit_counts", {})
        orbit_stabs = saved_d.get("orbit_stabs", {})
        LOG.add("task10d", f"Resuming from grid {start_idx:,}", level="info")

    LOG.add("task10d", f"[1/2] Canon + stab ({n_sample:,} grids, sequentiel)...", level="info")
    if progress_callback:
        progress_callback({"phase": "canonicalize", "percent": 1,
                           "message": f"Canon 0/{n_sample:,}..."})

    errors = 0
    last_save = time.time()

    for i in range(start_idx, n_sample):
        if stop_event and stop_event.is_set():
            LOG.add("task10d", f"STOPPED at {i:,}/{n_sample:,}", level="warning")
            ckpt_d.save({
                "status": "running", "processed": i,
                "orbit_counts": orbit_counts, "orbit_stabs": orbit_stabs,
            })
            return {"status": "paused"}

        try:
            h, _, stab = fast_canonicalize_and_stab(mates_sample[i])
        except Exception as e:
            errors += 1
            if errors <= 5:
                LOG.add("task10d", f"Canon error #{errors}: {e}", level="error")
            continue

        if h not in orbit_counts:
            orbit_counts[h] = 0
            orbit_stabs[h] = stab
            orbit_grids[h] = mates_sample[i]
        orbit_counts[h] += 1

        done = i + 1
        if done % 500 == 0 or done == n_sample:
            elapsed = time.time() - t0
            rate = (done - start_idx) / max(0.1, elapsed)
            eta = (n_sample - done) / max(1, rate)
            n_orb = len(orbit_counts)
            top_s = max(orbit_stabs.values()) if orbit_stabs else 0
            LOG.add("task10d",
                     f"  {done:,}/{n_sample:,} — {n_orb:,} orbits, top={top_s} "
                     f"({rate:.1f}/s, ETA {eta/60:.0f}min)")
            if progress_callback:
                pct = 1 + round(80 * done / n_sample)
                progress_callback({"phase": "canonicalize", "percent": pct,
                                   "message": f"Canon {done:,}/{n_sample:,} — {n_orb:,} orbits"})

        now = time.time()
        if now - last_save > 60:
            last_save = now
            ckpt_d.save({
                "status": "running", "processed": done,
                "orbit_counts": orbit_counts, "orbit_stabs": orbit_stabs,
            })

    n_orbits = len(orbit_counts)
    LOG.add("task10d", f"[1/2] Done: {n_orbits:,} orbits ({errors} errors)", level="success")

    LOG.add("task10d", f"[2/2] Swap check ({n_orbits:,} orbits)...", level="info")
    if progress_callback:
        progress_callback({"phase": "swap", "percent": 82,
                           "message": f"Swap 0/{n_orbits:,}"})

    orbit_self_dual = {}
    for idx, h in enumerate(orbit_grids):
        try:
            swapped = swap_grid(orbit_grids[h])
            swap_h, _, _ = fast_canonicalize_and_stab(swapped)
            orbit_self_dual[h] = (h == swap_h)
        except:
            orbit_self_dual[h] = False
        if (idx + 1) % 100 == 0 or (idx + 1) == n_orbits:
            if progress_callback:
                pct = 82 + round(15 * (idx + 1) / max(1, n_orbits))
                progress_callback({"phase": "swap", "percent": pct,
                                   "message": f"Swap {idx+1:,}/{n_orbits:,}"})

    self_dual_count = sum(1 for v in orbit_self_dual.values() if v)
    LOG.add("task10d", f"[2/2] Self-dual: {self_dual_count}/{n_orbits:,}", level="math")

    elapsed = round(time.time() - t0, 1)

    stab_dist_struct = {}
    stab_dist_total = {}
    for h in orbit_stabs:
        s = orbit_stabs[h]
        sd = orbit_self_dual.get(h, False)
        total = s * (2 if sd else 1)
        stab_dist_struct[s] = stab_dist_struct.get(s, 0) + 1
        stab_dist_total[total] = stab_dist_total.get(total, 0) + 1

    sorted_hashes = sorted(orbit_counts.keys(),
                           key=lambda h: -(orbit_stabs[h] * (2 if orbit_self_dual.get(h) else 1)))

    top_stab = 0
    if sorted_hashes:
        h0 = sorted_hashes[0]
        top_stab = orbit_stabs[h0] * (2 if orbit_self_dual.get(h0) else 1)

    LOG.add("task10d", "=" * 60, level="success")
    LOG.add("task10d", f"DONE in {elapsed:.0f}s ({elapsed/60:.1f} min)", level="success")
    LOG.add("task10d", f"{n_sample:,} grids -> {n_orbits:,} orbits", level="success")
    LOG.add("task10d", f"Top |Stab| total = {top_stab}", level="success")
    LOG.add("task10d", f"Self-dual: {self_dual_count}/{n_orbits:,}", level="math")

    LOG.add("task10d", "Stab distribution (structural):", level="math")
    for s in sorted(stab_dist_struct.keys(), reverse=True):
        LOG.add("task10d", f"  |Stab|={s}: {stab_dist_struct[s]} orbits", level="info")

    LOG.add("task10d", "Top 20 orbits:", level="math")
    for h in sorted_hashes[:20]:
        s = orbit_stabs[h]
        sd = orbit_self_dual.get(h, False)
        total = s * (2 if sd else 1)
        LOG.add("task10d",
                f"  {h[:16]}  stab={s}  {'SD' if sd else '  '}  total={total}  x{orbit_counts[h]}",
                level="success" if total >= 100 else "info")

    export_orbits = []
    for h in sorted_hashes:
        s = orbit_stabs[h]
        sd = orbit_self_dual.get(h, False)
        total = s * (2 if sd else 1)
        entry = {
            "canon_hash": h[:16],
            "stab_structural": s,
            "self_dual": sd,
            "stab_total": total,
            "count_in_sample": orbit_counts[h],
            "orbit_size_structural": TOTAL_STRUCTURAL // s,
        }
        if h in orbit_grids:
            entry["grid"] = grid_to_display(orbit_grids[h])
        export_orbits.append(entry)

    export_data = {
        "description": "All orbits from back-circulant 100k UNBIASED sample",
        "bias": "none (no r180)",
        "n_mates_total": n_total,
        "n_mates_breaking": n_breaking,
        "is_exact": is_exact,
        "n_sample": n_sample,
        "n_orbits": n_orbits,
        "self_dual_count": self_dual_count,
        "top_stab": top_stab,
        "stab_distribution_structural": {str(k): v for k, v in sorted(stab_dist_struct.items(), reverse=True)},
        "stab_distribution_total": {str(k): v for k, v in sorted(stab_dist_total.items(), reverse=True)},
        "orbits": export_orbits,
        "elapsed_seconds": elapsed,
        "timestamp": datetime.now().isoformat(),
    }

    try:
        export_path = EXPORT_DIR / "task10d_unbiased_analysis.json"
        export_path.parent.mkdir(parents=True, exist_ok=True)
        with open(export_path, "w") as f:
            json.dump(export_data, f, indent=2, default=str)
        LOG.add("task10d", f"Export -> {export_path}", level="info")
    except Exception as e:
        LOG.add("task10d", f"Export error: {e}", level="error")

    result = {
        "status": "done",
        "source": "back_circulant",
        "bias": "none",
        "n_mates_total": n_total,
        "n_sample": n_sample,
        "n_orbits": n_orbits,
        "self_dual_count": self_dual_count,
        "top_stab": top_stab,
        "stab_distribution": {str(k): v for k, v in sorted(stab_dist_total.items(), reverse=True)},
        "elapsed": elapsed,
    }
    ckpt_d.save(result)

    if progress_callback:
        progress_callback({"percent": 100, "phase": "done",
                           "message": f"Done: {n_orbits:,} orbits, top={top_stab}"})
    LOG.add("task10d", f"DONE — {n_orbits:,} orbits, top |Stab|={top_stab} ({elapsed:.0f}s)", level="success")
    return result


# =============================================================
# Grid Serialization
# =============================================================

def _serialize_grid(grid):
    """Grid -> list of lists of [d, c]."""
    result = []
    for row in grid:
        r = []
        for cell in row:
            if isinstance(cell, (tuple, list)):
                r.append([int(cell[0]), int(cell[1])])
            elif isinstance(cell, dict):
                r.append([cell['d'], cell['c']])
            else:
                r.append(cell)
        result.append(r)
    return result


def _deserialize_grid(data):
    """List -> tuple of tuples."""
    return tuple(
        tuple(
            (cell[0], cell[1]) if isinstance(cell, list) else cell
            for cell in row
        )
        for row in data
    )


# =============================================================
# Task 10E — C3-involution mates (exhaustive)
# =============================================================

def task10e_c3_mates(progress_callback=None, stop_event=None):
    """Task 10E — Count ALL color mates with C3-involution symmetry."""
    ckpt = CheckpointManager("task10e_c3_mates")
    saved = ckpt.load()
    if saved and saved.get("status") == "done":
        return saved

    LOG.add("task10e", "=" * 60, level="success")
    LOG.add("task10e", "TASK 10E — ALL MATES (C3 INVOLUTION BIAS)", level="success")
    LOG.add("task10e", "=" * 60, level="success")
    LOG.add("task10e", "C3: order-2 involution (NOT r180), 9 fixed cells", level="info")
    LOG.add("task10e", "Breaking: color[0][0]=0 → result × 9 = exact total", level="math")

    start = time.time()
    if progress_callback:
        progress_callback({"phase": "solving", "percent": 5, "message": "CP-SAT solving (C3 bias)..."})

    def _on_mate_found(n, elapsed):
        rate = n / max(1, elapsed)
        if n <= 100 or n % 1_000_000 == 0:
            LOG.add("task10e", f"  {n:,} mates ({elapsed:.0f}s, {rate:.0f}/s)", level="info")
        if progress_callback:
            progress_callback({
                "phase": "solving",
                "percent": min(95, 5 + int(elapsed / 120)),
                "message": f"Counting... {n:,} mates ({elapsed:.0f}s, {rate:.0f}/s)",
                "found": n,
            })
        if n % 100000 == 0:
            ckpt.save({"status": "running", "n_breaking_so_far": n, "elapsed_so_far": elapsed})

    mates_sample, n_breaking, status = _find_all_color_mates(
        BACK_CIRCULANT, exhaustive=True, timeout=86400,
        on_solution_found=_on_mate_found, sample_size=100000,
        use_rot180=False, symmetry_fn=add_C3_involution_color_only)

    from ortools.sat.python import cp_model as cpm
    elapsed = round(time.time() - start, 1)
    is_exact = status in (cpm.OPTIMAL, cpm.FEASIBLE) and elapsed < 86000
    n_total = n_breaking * 9

    LOG.add("task10e", f"RESULT: {n_breaking:,} breaking × 9 = {n_total:,} total", level="success")
    LOG.add("task10e", f"Status: {'EXACT' if is_exact else 'INCOMPLETE'}", level="success" if is_exact else "warning")
    LOG.add("task10e", f"Sample: {len(mates_sample)} grids | Time: {elapsed}s", level="info")

    result = {
        "status": "done", "source": "back_circulant", "bias": "C3_involution",
        "n_mates_breaking": n_breaking, "n_mates_total": n_total,
        "is_exact": is_exact, "n_sample": len(mates_sample),
        "sample": [_serialize_grid(m) for m in mates_sample], "elapsed": elapsed,
    }
    ckpt.save(result)
    try:
        with open(EXPORT_DIR / "task10e_c3_mates.json", "w") as f:
            json.dump({"description": "C3-symmetric mates for BC", "bias": "C3_involution",
                        "n_mates_breaking": n_breaking, "n_mates_total": n_total,
                        "is_exact": is_exact, "n_sample": len(mates_sample),
                        "elapsed": elapsed, "timestamp": datetime.now().isoformat()}, f, indent=2, default=str)
    except Exception as e:
        LOG.add("task10e", f"Export error: {e}", level="error")

    if progress_callback:
        progress_callback({"percent": 100, "phase": "done"})
    return result


# =============================================================
# Task 10F — C4-transpose mates (exhaustive)
# =============================================================

def task10f_c4_mates(progress_callback=None, stop_event=None):
    """Task 10F — Count ALL color mates with C4-transpose symmetry."""
    ckpt = CheckpointManager("task10f_c4_mates")
    saved = ckpt.load()
    if saved and saved.get("status") == "done":
        return saved

    LOG.add("task10f", "=" * 60, level="success")
    LOG.add("task10f", "TASK 10F — ALL MATES (C4 TRANSPOSE BIAS)", level="success")
    LOG.add("task10f", "=" * 60, level="success")
    LOG.add("task10f", "C4: order-2 transpose involution, 9 fixed cells", level="info")
    LOG.add("task10f", "Breaking: color[0][0]=0 → result × 9 = exact total", level="math")

    start = time.time()
    if progress_callback:
        progress_callback({"phase": "solving", "percent": 5, "message": "CP-SAT solving (C4 bias)..."})

    def _on_mate_found(n, elapsed):
        rate = n / max(1, elapsed)
        if n <= 100 or n % 1_000_000 == 0:
            LOG.add("task10f", f"  {n:,} mates ({elapsed:.0f}s, {rate:.0f}/s)", level="info")
        if progress_callback:
            progress_callback({
                "phase": "solving",
                "percent": min(95, 5 + int(elapsed / 120)),
                "message": f"Counting... {n:,} mates ({elapsed:.0f}s, {rate:.0f}/s)",
                "found": n,
            })
        if n % 100000 == 0:
            ckpt.save({"status": "running", "n_breaking_so_far": n, "elapsed_so_far": elapsed})

    mates_sample, n_breaking, status = _find_all_color_mates(
        BACK_CIRCULANT, exhaustive=True, timeout=86400,
        on_solution_found=_on_mate_found, sample_size=100000,
        use_rot180=False, symmetry_fn=add_C4_transpose_color_only)

    from ortools.sat.python import cp_model as cpm
    elapsed = round(time.time() - start, 1)
    is_exact = status in (cpm.OPTIMAL, cpm.FEASIBLE) and elapsed < 86000
    n_total = n_breaking * 9

    LOG.add("task10f", f"RESULT: {n_breaking:,} breaking × 9 = {n_total:,} total", level="success")
    LOG.add("task10f", f"Status: {'EXACT' if is_exact else 'INCOMPLETE'}", level="success" if is_exact else "warning")
    LOG.add("task10f", f"Sample: {len(mates_sample)} grids | Time: {elapsed}s", level="info")

    result = {
        "status": "done", "source": "back_circulant", "bias": "C4_transpose",
        "n_mates_breaking": n_breaking, "n_mates_total": n_total,
        "is_exact": is_exact, "n_sample": len(mates_sample),
        "sample": [_serialize_grid(m) for m in mates_sample], "elapsed": elapsed,
    }
    ckpt.save(result)
    try:
        with open(EXPORT_DIR / "task10f_c4_mates.json", "w") as f:
            json.dump({"description": "C4-transpose mates for BC", "bias": "C4_transpose",
                        "n_mates_breaking": n_breaking, "n_mates_total": n_total,
                        "is_exact": is_exact, "n_sample": len(mates_sample),
                        "elapsed": elapsed, "timestamp": datetime.now().isoformat()}, f, indent=2, default=str)
    except Exception as e:
        LOG.add("task10f", f"Export error: {e}", level="error")

    if progress_callback:
        progress_callback({"percent": 100, "phase": "done"})
    return result


# =============================================================
# Task 10G — Order-3 cycle mates (exhaustive)
# =============================================================

def task10g_order3_mates(progress_callback=None, stop_event=None):
    """Task 10G — Count ALL color mates with order-3 cycle symmetry."""
    ckpt = CheckpointManager("task10g_order3_mates")
    saved = ckpt.load()
    if saved and saved.get("status") == "done":
        return saved

    LOG.add("task10g", "=" * 60, level="success")
    LOG.add("task10g", "TASK 10G — ALL MATES (ORDER-3 CYCLE BIAS)", level="success")
    LOG.add("task10g", "=" * 60, level="success")
    LOG.add("task10g", "Order-3: cols 0→1→2→0 per stack, 27 triplets", level="info")
    LOG.add("task10g", "UNIQUE: only bias catching pure order-3 stabs", level="info")
    LOG.add("task10g", "Breaking: color[0][0]=0 → result × 9 = exact total", level="math")

    start = time.time()
    if progress_callback:
        progress_callback({"phase": "solving", "percent": 5, "message": "CP-SAT solving (order-3 bias)..."})

    def _on_mate_found(n, elapsed):
        rate = n / max(1, elapsed)
        if n <= 100 or n % 1_000_000 == 0:
            LOG.add("task10g", f"  {n:,} mates ({elapsed:.0f}s, {rate:.0f}/s)", level="info")
        if progress_callback:
            progress_callback({
                "phase": "solving",
                "percent": min(95, 5 + int(elapsed / 120)),
                "message": f"Counting... {n:,} mates ({elapsed:.0f}s, {rate:.0f}/s)",
                "found": n,
            })
        if n % 100000 == 0:
            ckpt.save({"status": "running", "n_breaking_so_far": n, "elapsed_so_far": elapsed})

    mates_sample, n_breaking, status = _find_all_color_mates(
        BACK_CIRCULANT, exhaustive=True, timeout=86400,
        on_solution_found=_on_mate_found, sample_size=100000,
        use_rot180=False, symmetry_fn=add_order3_cycle_color_only)

    from ortools.sat.python import cp_model as cpm
    elapsed = round(time.time() - start, 1)
    is_exact = status in (cpm.OPTIMAL, cpm.FEASIBLE) and elapsed < 86000
    n_total = n_breaking * 9

    LOG.add("task10g", f"RESULT: {n_breaking:,} breaking × 9 = {n_total:,} total", level="success")
    LOG.add("task10g", f"Status: {'EXACT' if is_exact else 'INCOMPLETE'}", level="success" if is_exact else "warning")
    LOG.add("task10g", f"Sample: {len(mates_sample)} grids | Time: {elapsed}s", level="info")

    result = {
        "status": "done", "source": "back_circulant", "bias": "order3_cycle",
        "n_mates_breaking": n_breaking, "n_mates_total": n_total,
        "is_exact": is_exact, "n_sample": len(mates_sample),
        "sample": [_serialize_grid(m) for m in mates_sample], "elapsed": elapsed,
    }
    ckpt.save(result)
    try:
        with open(EXPORT_DIR / "task10g_order3_mates.json", "w") as f:
            json.dump({"description": "Order-3 cycle mates for BC", "bias": "order3_cycle",
                        "n_mates_breaking": n_breaking, "n_mates_total": n_total,
                        "is_exact": is_exact, "n_sample": len(mates_sample),
                        "elapsed": elapsed, "timestamp": datetime.now().isoformat()}, f, indent=2, default=str)
    except Exception as e:
        LOG.add("task10g", f"Export error: {e}", level="error")

    if progress_callback:
        progress_callback({"percent": 100, "phase": "done"})
    return result


# =============================================================
# Task 10G Partitioned — 8 parallel workers (subprocess)
# =============================================================

def _task10g_partition(part_id, progress_callback=None, stop_event=None):
    """Task 10G partition: launches 2 worker subprocesses (a+b) per partition.
    True parallelism — each worker has its own Python process + GIL.
    Uses 2 cores per partition = 16 cores total for 8 partitions."""
    import subprocess

    task_label = f"task10g_p{part_id}"
    ckpt_a_name = f"task10g_p{part_id}a_order3"
    ckpt_b_name = f"task10g_p{part_id}b_order3"
    ckpt_a = CheckpointManager(ckpt_a_name)
    ckpt_b = CheckpointManager(ckpt_b_name)

    # Check if both already done
    saved_a = ckpt_a.load()
    saved_b = ckpt_b.load()
    if (saved_a and saved_a.get("status") == "done" and
        saved_b and saved_b.get("status") == "done"):
        # Combine results
        total = saved_a["n_mates_breaking"] + saved_b["n_mates_breaking"]
        return {
            "status": "done", "partition": part_id,
            "n_mates_breaking": total,
            "is_exact": saved_a.get("is_exact", False) and saved_b.get("is_exact", False),
            "n_sample": saved_a.get("n_sample", 0) + saved_b.get("n_sample", 0),
            "elapsed": max(saved_a.get("elapsed", 0), saved_b.get("elapsed", 0)),
        }

    LOG.add(task_label, "=" * 60, level="success")
    LOG.add(task_label, f"TASK 10G-P{part_id} — 2 SUBPROCESSES (a+b)", level="success")
    LOG.add(task_label, "=" * 60, level="success")

    # Find worker script
    import sys
    worker_path = Path(__file__).parent / "worker_10g.py"
    if not worker_path.exists():
        msg = f"worker_10g.py not found at {worker_path}"
        LOG.add(task_label, msg, level="error")
        if progress_callback:
            progress_callback({"percent": 0, "message": msg})
        return {"status": "error", "message": msg}

    # Launch 2 subprocesses
    procs = {}
    logs = {}
    for sub in ('a', 'b'):
        ckpt_sub = ckpt_a if sub == 'a' else ckpt_b
        saved_sub = ckpt_sub.load()
        if saved_sub and saved_sub.get("status") == "done":
            LOG.add(task_label, f"  P{part_id}{sub} already done, skipping", level="info")
            continue
        log_path = Path(__file__).parent / f"log_10g_p{part_id}{sub}.txt"
        log_file = open(log_path, "w")
        proc = subprocess.Popen(
            [sys.executable, str(worker_path), str(part_id), sub],
            stdout=log_file, stderr=subprocess.STDOUT,
            cwd=str(Path(__file__).parent)
        )
        procs[sub] = proc
        logs[sub] = log_file
        LOG.add(task_label, f"  P{part_id}{sub} PID={proc.pid}", level="info")

    if progress_callback:
        n_procs = len(procs)
        progress_callback({"phase": "solving", "percent": 5,
                           "message": f"P{part_id}: {n_procs} subprocess(es) running"})

    # Poll checkpoints until all done
    start = time.time()
    while procs:
        if stop_event and stop_event.is_set():
            LOG.add(task_label, f"STOP requested, killing subprocesses", level="warning")
            for sub, proc in procs.items():
                proc.terminate()
                try:
                    proc.wait(timeout=10)
                except:
                    proc.kill()
            for lf in logs.values():
                lf.close()
            return {"status": "paused"}

        time.sleep(3)

        # Check which procs finished
        done_subs = []
        for sub, proc in procs.items():
            if proc.poll() is not None:
                done_subs.append(sub)
        for sub in done_subs:
            logs[sub].close()
            del procs[sub]
            del logs[sub]
            LOG.add(task_label, f"  P{part_id}{sub} subprocess finished", level="info")

        # Read checkpoints for combined progress
        total_so_far = 0
        details = []
        for sub in ('a', 'b'):
            ckpt_sub = ckpt_a if sub == 'a' else ckpt_b
            cp = ckpt_sub.load()
            if cp:
                if cp.get("status") == "done":
                    n = cp.get("n_mates_breaking", 0)
                    total_so_far += n
                    details.append(f"{sub}={n:,}✓")
                elif cp.get("status") == "running":
                    n = cp.get("n_breaking_so_far", 0)
                    total_so_far += n
                    details.append(f"{sub}={n:,}")

        elapsed = time.time() - start
        rate = total_so_far / max(1, elapsed)
        still_running = len(procs)
        detail_str = " + ".join(details) if details else "..."
        LOG.add(task_label, f"  P{part_id}: {total_so_far:,} ({detail_str}) — {rate:.0f}/s, {still_running} running", level="info")
        if progress_callback:
            progress_callback({
                "phase": "solving",
                "percent": min(95, 5 + int(elapsed / 120)),
                "message": f"P{part_id}: {total_so_far:,} mates ({rate:.0f}/s)",
                "found": total_so_far,
            })

    # All done — wait for file system to flush, then read with retries
    LOG.add(task_label, f"  All subprocesses exited, waiting for checkpoints...", level="info")
    time.sleep(5)  # Let Windows flush file buffers

    elapsed = round(time.time() - start, 1)

    # Retry reading checkpoints up to 5 times
    total_breaking = 0
    is_exact = True
    total_sample = 0
    for attempt in range(5):
        total_breaking = 0
        is_exact = True
        total_sample = 0
        all_done = True
        final_a = ckpt_a.load()
        final_b = ckpt_b.load()
        for cp in (final_a, final_b):
            if cp and cp.get("status") == "done":
                total_breaking += cp.get("n_mates_breaking", 0)
                total_sample += cp.get("n_sample", 0)
                if not cp.get("is_exact", False):
                    is_exact = False
            else:
                all_done = False
                is_exact = False
                # Also count running checkpoint data
                if cp and cp.get("status") == "running":
                    total_breaking += cp.get("n_breaking_so_far", 0)

        if all_done:
            LOG.add(task_label, f"  Checkpoints read OK (attempt {attempt+1})", level="info")
            break
        else:
            LOG.add(task_label, f"  Retry {attempt+1}/5: not all checkpoints 'done' yet...", level="warning")
            time.sleep(3)

    # Save combined result as the partition checkpoint (for merge compatibility)
    combined = {
        "status": "done", "source": "back_circulant", "bias": "order3_cycle",
        "partition": part_id, "method": "dual_subprocess",
        "n_mates_breaking": total_breaking,
        "is_exact": is_exact,
        "n_sample_a": final_a.get("n_sample", 0) if final_a else 0,
        "n_sample_b": final_b.get("n_sample", 0) if final_b else 0,
        "elapsed": elapsed,
    }
    CheckpointManager(f"task10g_p{part_id}_order3").save(combined)

    LOG.add(task_label, f"DONE P{part_id}: {total_breaking:,} breaking ({elapsed:.0f}s)", level="success")
    if progress_callback:
        progress_callback({"percent": 100, "phase": "done",
                           "message": f"P{part_id} done: {total_breaking:,} breaking"})
    return combined


# Factory: create the 8 task functions
def _make_task10g_part(pid):
    def task_fn(progress_callback=None, stop_event=None):
        return _task10g_partition(pid, progress_callback, stop_event)
    task_fn.__name__ = f"task10g_p{pid}_order3_mates"
    return task_fn

task10g_p1 = _make_task10g_part(1)
task10g_p2 = _make_task10g_part(2)
task10g_p3 = _make_task10g_part(3)
task10g_p4 = _make_task10g_part(4)
task10g_p5 = _make_task10g_part(5)
task10g_p6 = _make_task10g_part(6)
task10g_p7 = _make_task10g_part(7)
task10g_p8 = _make_task10g_part(8)


def task10g_merge(progress_callback=None, stop_event=None):
    """Merge all 8 partitions of Task 10G into a single result.
    Combines counts and performs weighted reservoir sampling on samples."""
    ckpt = CheckpointManager("task10g_merged")
    saved = ckpt.load()
    if saved and saved.get("status") == "done":
        return saved

    LOG.add("task10g_merge", "=" * 60, level="success")
    LOG.add("task10g_merge", "TASK 10G MERGE — Combining 8 partitions", level="success")
    LOG.add("task10g_merge", "=" * 60, level="success")

    total_breaking = 0
    all_exact = True
    total_elapsed = 0
    partition_results = {}
    all_samples = []  # (weight, sample_list) for weighted merge

    for pid in range(1, 9):
        part_total = 0
        part_exact = True
        part_elapsed = 0

        for sub in ('a', 'b'):
            ckpt_name = f"task10g_p{pid}{sub}_order3"
            ckpt_p = CheckpointManager(ckpt_name)
            sp = ckpt_p.load()
            if not sp:
                LOG.add("task10g_merge", f"  P{pid}{sub}: NOT FOUND", level="error")
                part_exact = False
                all_exact = False
                continue
            if sp.get("status") == "running":
                n = sp.get("n_breaking_so_far", 0)
                LOG.add("task10g_merge", f"  P{pid}{sub}: RUNNING ({n:,} so far)", level="warning")
                part_total += n
                part_exact = False
                all_exact = False
                continue
            if sp.get("status") != "done":
                LOG.add("task10g_merge", f"  P{pid}{sub}: status={sp.get('status')}", level="warning")
                part_exact = False
                all_exact = False
                continue

            nb = sp["n_mates_breaking"]
            part_total += nb
            part_elapsed = max(part_elapsed, sp.get("elapsed", 0))
            if not sp.get("is_exact", False):
                part_exact = False
                all_exact = False
            LOG.add("task10g_merge", f"  P{pid}{sub}: {nb:,} breaking ({'exact' if sp.get('is_exact') else 'partial'})", level="info")

            # Collect samples — try separate sample file first, fallback to inline
            sample_file = sp.get("sample_file")
            if sample_file:
                sample_path = CHECKPOINT_DIR / sample_file
                try:
                    with open(sample_path) as sf:
                        sample_data = json.load(sf)
                    all_samples.append((nb, sample_data))
                    LOG.add("task10g_merge", f"    Loaded {len(sample_data):,} grids from {sample_file}", level="info")
                except Exception as e:
                    LOG.add("task10g_merge", f"    Sample file error: {e}", level="error")
            elif "sample" in sp and sp["sample"]:
                all_samples.append((nb, sp["sample"]))

        total_breaking += part_total
        total_elapsed = max(total_elapsed, part_elapsed)
        partition_results[pid] = {"n_breaking": part_total, "is_exact": part_exact}
        LOG.add("task10g_merge", f"  P{pid} combined: {part_total:,} breaking", level="info")

    n_total = total_breaking * 9
    n_partitions_done = sum(1 for v in partition_results.values() if v.get("is_exact", False))

    LOG.add("task10g_merge", f"Partitions done: {n_partitions_done}/8", level="info")
    LOG.add("task10g_merge", f"TOTAL: {total_breaking:,} breaking × 9 = {n_total:,} total", level="success")
    LOG.add("task10g_merge", f"All exact: {all_exact}", level="success" if all_exact else "warning")

    # Concatenate ALL samples from all 16 sub-partitions (up to 1.6M)
    merged_sample = []
    if all_samples:
        for weight, sample_list in all_samples:
            merged_sample.extend(sample_list)
        LOG.add("task10g_merge", f"Total sample: {len(merged_sample):,} grids (all kept)", level="info")

    result = {
        "status": "done" if all_exact else "partial",
        "source": "back_circulant", "bias": "order3_cycle",
        "method": "partitioned_8",
        "n_mates_breaking": total_breaking,
        "n_mates_total": n_total,
        "is_exact": all_exact,
        "n_partitions_done": n_partitions_done,
        "partition_results": partition_results,
        "n_sample": len(merged_sample),
        "elapsed_max": total_elapsed,
    }
    ckpt.save(result)

    # Also save as the "official" task10g checkpoint for 10Gb compatibility
    # (10Gb parallel reads sub-partition checkpoints directly, not this one)
    if all_exact:
        compat_result = {
            "status": "done", "source": "back_circulant", "bias": "order3_cycle",
            "n_mates_breaking": total_breaking, "n_mates_total": n_total,
            "is_exact": True, "n_sample": len(merged_sample), "elapsed": total_elapsed,
        }
        CheckpointManager("task10g_order3_mates").save(compat_result)
        LOG.add("task10g_merge", "Saved compat checkpoint → task10g_order3_mates", level="info")

    try:
        with open(EXPORT_DIR / "task10g_merged.json", "w") as f:
            json.dump({"description": "Order-3 cycle mates (partitioned merge)",
                        "n_mates_breaking": total_breaking, "n_mates_total": n_total,
                        "is_exact": all_exact, "n_sample": len(merged_sample),
                        "n_partitions_done": n_partitions_done,
                        "partition_results": partition_results,
                        "elapsed_max": total_elapsed,
                        "timestamp": datetime.now().isoformat()}, f, indent=2, default=str)
    except Exception as e:
        LOG.add("task10g_merge", f"Export error: {e}", level="error")

    if progress_callback:
        progress_callback({"percent": 100, "phase": "done",
                           "message": f"Merged: {total_breaking:,} × 9 = {n_total:,}"})
    return result


# =============================================================
# Generic analysis for 10Eb / 10Fb / 10Gb
# =============================================================

def _task10x_analyze_sample(task_label, source_ckpt_name, ckpt_name,
                             progress_callback=None, stop_event=None):
    """Generic: Canonicalize sample from a Task 10x counting run."""
    ckpt_ab = CheckpointManager(ckpt_name)
    saved_ab = ckpt_ab.load()
    if saved_ab and saved_ab.get("status") == "done":
        return saved_ab

    if not canon_lib_available():
        msg = "canon.dll/.so introuvable."
        LOG.add(task_label, msg, level="error")
        if progress_callback:
            progress_callback({"percent": 0, "message": msg})
        return {"status": "error", "message": msg}

    ckpt_src = CheckpointManager(source_ckpt_name)
    saved_src = ckpt_src.load()
    if not saved_src or saved_src.get("status") != "done":
        msg = f"Source task ({source_ckpt_name}) must be completed first."
        LOG.add(task_label, msg, level="error")
        if progress_callback:
            progress_callback({"percent": 0, "message": msg})
        return {"status": "error", "message": msg}

    n_breaking = saved_src["n_mates_breaking"]
    n_total = saved_src["n_mates_total"]
    is_exact = saved_src["is_exact"]
    bias = saved_src.get("bias", "unknown")
    raw_sample = ckpt_src.load_sample()
    if not raw_sample:
        msg = f"No sample data found for {source_ckpt_name}."
        LOG.add(task_label, msg, level="error")
        if progress_callback:
            progress_callback({"percent": 0, "message": msg})
        return {"status": "error", "message": msg}
    mates_sample = [_deserialize_grid(g) for g in raw_sample]
    n_sample = len(mates_sample)

    LOG.add(task_label, "=" * 60, level="success")
    LOG.add(task_label, f"TASK {task_label.upper()} — CANONICALIZE SAMPLE", level="success")
    LOG.add(task_label, "=" * 60, level="success")
    LOG.add(task_label, f"Source: {n_total:,} total mates ({bias}), sample={n_sample:,}", level="info")

    t0 = time.time()

    start_idx = 0
    orbit_counts = {}
    orbit_stabs = {}
    orbit_grids = {}
    if saved_ab and saved_ab.get("status") == "running":
        start_idx = saved_ab.get("processed", 0)
        orbit_counts = saved_ab.get("orbit_counts", {})
        orbit_stabs = saved_ab.get("orbit_stabs", {})
        LOG.add(task_label, f"Resuming from grid {start_idx:,}", level="info")

    LOG.add(task_label, f"[1/2] Canon + stab ({n_sample:,} grids)...", level="info")
    if progress_callback:
        progress_callback({"phase": "canonicalize", "percent": 1,
                           "message": f"Canon 0/{n_sample:,}..."})

    errors = 0
    last_save = time.time()

    for i in range(start_idx, n_sample):
        if stop_event and stop_event.is_set():
            LOG.add(task_label, f"STOPPED at {i:,}/{n_sample:,}", level="warning")
            ckpt_ab.save({
                "status": "running", "processed": i,
                "orbit_counts": orbit_counts, "orbit_stabs": orbit_stabs,
            })
            return {"status": "paused"}

        try:
            h, _, stab = fast_canonicalize_and_stab(mates_sample[i])
        except Exception as e:
            errors += 1
            if errors <= 5:
                LOG.add(task_label, f"Canon error #{errors}: {e}", level="error")
            continue

        if h not in orbit_counts:
            orbit_counts[h] = 0
            orbit_stabs[h] = stab
            orbit_grids[h] = mates_sample[i]
        orbit_counts[h] += 1

        done = i + 1
        if done % 500 == 0 or done == n_sample:
            elapsed = time.time() - t0
            rate = (done - start_idx) / max(0.1, elapsed)
            eta = (n_sample - done) / max(1, rate)
            n_orb = len(orbit_counts)
            top_s = max(orbit_stabs.values()) if orbit_stabs else 0
            LOG.add(task_label,
                     f"  {done:,}/{n_sample:,} — {n_orb:,} orbits, top|Stab|={top_s} "
                     f"({rate:.1f}/s, ETA {eta:.0f}s)", level="info")
            if progress_callback:
                pct = 5 + int(85 * done / n_sample)
                progress_callback({
                    "phase": "canonicalize", "percent": pct,
                    "message": f"Canon {done:,}/{n_sample:,} — {n_orb:,} orbits",
                    "n_orbits": n_orb,
                })

        if time.time() - last_save > 120:
            ckpt_ab.save({
                "status": "running", "processed": done,
                "orbit_counts": orbit_counts, "orbit_stabs": orbit_stabs,
            })
            last_save = time.time()

    n_orbits = len(orbit_counts)
    LOG.add(task_label, f"[1/2] Done: {n_orbits:,} orbits ({errors} errors)", level="success")

    # PHASE 2: SWAP CHECK
    LOG.add(task_label, f"[2/2] Swap check ({n_orbits:,} orbits)...", level="info")
    self_dual_count = 0
    orbit_sd = {}
    for h in orbit_grids:
        grid = orbit_grids[h]
        if grid is None:
            orbit_sd[h] = False
            continue
        swapped = tuple(tuple((cell[1], cell[0]) for cell in row) for row in grid)
        try:
            h_swap, _, _ = fast_canonicalize_and_stab(swapped)
            orbit_sd[h] = (h_swap == h)
            if h_swap == h:
                self_dual_count += 1
        except:
            orbit_sd[h] = False

    LOG.add(task_label, f"[2/2] Self-dual: {self_dual_count}/{n_orbits:,}", level="math")

    stab_dist_struct = {}
    stab_dist_total = {}
    for h in orbit_counts:
        ss = orbit_stabs[h]
        st = ss * 2 if orbit_sd.get(h, False) else ss
        stab_dist_struct[ss] = stab_dist_struct.get(ss, 0) + 1
        stab_dist_total[st] = stab_dist_total.get(st, 0) + 1

    top_stab = max(stab_dist_total.keys()) if stab_dist_total else 0
    elapsed = round(time.time() - t0, 1)

    orbits_list = []
    for h in sorted(orbit_counts, key=lambda x: -orbit_stabs[x]):
        ss = orbit_stabs[h]
        sd = orbit_sd.get(h, False)
        st = ss * 2 if sd else ss
        orbits_list.append({
            "canon_hash": h, "stab_structural": ss, "stab_total": st,
            "self_dual": sd, "sample_count": orbit_counts[h],
            "orbit_size": TOTAL_STRUCTURAL // ss,
        })

    LOG.add(task_label, "=" * 60, level="success")
    LOG.add(task_label, f"DONE in {elapsed:.0f}s ({elapsed/60:.1f} min)", level="success")
    LOG.add(task_label, f"{n_sample:,} grids → {n_orbits:,} orbits", level="success")
    LOG.add(task_label, f"Top |Stab| total = {top_stab}", level="success")
    LOG.add(task_label, f"Self-dual: {self_dual_count}/{n_orbits:,}", level="math")
    LOG.add(task_label, "Stab distribution (total):", level="math")
    for s in sorted(stab_dist_total, reverse=True):
        LOG.add(task_label, f"  |Stab|={s}: {stab_dist_total[s]} orbits", level="info")

    result = {
        "status": "done", "source": "back_circulant", "bias": bias,
        "n_mates_total": n_total, "is_exact": is_exact,
        "n_sample": n_sample, "n_orbits": n_orbits,
        "self_dual_count": self_dual_count, "top_stab": top_stab,
        "stab_dist_structural": stab_dist_struct,
        "stab_dist_total": stab_dist_total,
        "orbits": orbits_list, "elapsed": elapsed,
    }
    ckpt_ab.save(result)
    try:
        export_path = EXPORT_DIR / f"{ckpt_name}.json"
        export_path.parent.mkdir(parents=True, exist_ok=True)
        with open(export_path, "w") as f:
            json.dump(result, f, indent=2, default=str)
        LOG.add(task_label, f"Export -> {export_path}", level="info")
    except Exception as e:
        LOG.add(task_label, f"Export error: {e}", level="error")

    if progress_callback:
        progress_callback({"percent": 100, "phase": "done",
                           "message": f"Done: {n_orbits:,} orbits"})
    return result


def task10eb_c3_analysis(progress_callback=None, stop_event=None):
    """Task 10Eb — Analyze C3 sample."""
    return _task10x_analyze_sample("task10eb", "task10e_c3_mates", "task10eb_c3_analysis",
                                    progress_callback, stop_event)

def task10fb_c4_analysis(progress_callback=None, stop_event=None):
    """Task 10Fb — Analyze C4 sample."""
    return _task10x_analyze_sample("task10fb", "task10f_c4_mates", "task10fb_c4_analysis",
                                    progress_callback, stop_event)

def task10gb_order3_analysis(progress_callback=None, stop_event=None):
    """Task 10Gb — Analyze order-3 sample (parallel: 16 subprocesses).
    Each subprocess canonicalizes one sub-partition's 100k sample.
    Results are merged into unified orbit analysis."""
    import subprocess
    import sys

    ckpt = CheckpointManager("task10gb_order3_analysis")
    saved = ckpt.load()
    if saved and saved.get("status") == "done":
        return saved

    if not canon_lib_available():
        msg = "canon.dll/.so introuvable."
        LOG.add("task10gb", msg, level="error")
        if progress_callback:
            progress_callback({"percent": 0, "message": msg})
        return {"status": "error", "message": msg}

    worker_path = Path(__file__).parent / "worker_10gb.py"
    if not worker_path.exists():
        msg = f"worker_10gb.py not found"
        LOG.add("task10gb", msg, level="error")
        if progress_callback:
            progress_callback({"percent": 0, "message": msg})
        return {"status": "error", "message": msg}

    LOG.add("task10gb", "=" * 60, level="success")
    LOG.add("task10gb", f"TASK 10Gb — CANONICALIZATION ({len(sub_parts)} sub-partitions)", level="success")
    LOG.add("task10gb", "=" * 60, level="success")

    # Collect which sub-partitions have data
    sub_parts = []
    for pid in range(1, 9):
        for sub in ('a', 'b'):
            src_ckpt = CheckpointManager(f"task10g_p{pid}{sub}_order3")
            src = src_ckpt.load()
            if src and src.get("status") == "done" and (src.get("sample") or src.get("sample_file")):
                sub_parts.append((pid, sub))

    if not sub_parts:
        # Fallback: try unified checkpoint
        src_ckpt = CheckpointManager("task10g_order3_mates")
        src = src_ckpt.load()
        if src and src.get("status") == "done" and (src.get("sample") or src.get("sample_file")):
            LOG.add("task10gb", "No sub-partitions found, falling back to sequential", level="warning")
            return _task10x_analyze_sample("task10gb", "task10g_order3_mates", "task10gb_order3_analysis",
                                            progress_callback, stop_event)
        msg = "No source data found. Run 10G partitions first."
        LOG.add("task10gb", msg, level="error")
        return {"status": "error", "message": msg}

    LOG.add("task10gb", f"Found {len(sub_parts)} sub-partitions to analyze", level="info")
    if progress_callback:
        progress_callback({"phase": "launching", "percent": 2,
                           "message": f"Launching {len(sub_parts)} workers..."})

    # Launch all workers at once
    t0 = time.time()
    procs = {}
    logs = {}
    for pid, sub in sub_parts:
        already = CheckpointManager(f"task10gb_p{pid}{sub}").load()
        if already and already.get("status") == "done":
            LOG.add("task10gb", f"  P{pid}{sub} already done, skip", level="info")
            continue
        log_path = Path(__file__).parent / f"log_10gb_p{pid}{sub}.txt"
        lf = open(log_path, "w")
        env = os.environ.copy()
        env["PYTHONUNBUFFERED"] = "1"
        proc = subprocess.Popen(
            [sys.executable, str(worker_path), str(pid), sub],
            stdout=lf, stderr=subprocess.STDOUT,
            cwd=str(Path(__file__).parent), env=env
        )
        procs[(pid, sub)] = proc
        logs[(pid, sub)] = lf

    LOG.add("task10gb", f"Launched {len(procs)} workers", level="info")

    n_total = len(sub_parts)
    last_summary_time = 0
    import re

    def _parse_10gb_log(pid, sub):
        """Read worker log, return (done, total, n_orbits, last_line)."""
        try:
            log_path = Path(__file__).parent / f"log_10gb_p{pid}{sub}.txt"
            with open(log_path, "r") as rf:
                lines = rf.readlines()
            for line in reversed(lines):
                line = line.strip()
                if not line:
                    continue
                m = re.search(r'(\d[\d,]*)/(\d[\d,]*)', line)
                m_orb = re.search(r'([\d,]+)\s*orbits', line)
                done = int(m.group(1).replace(',', '')) if m else 0
                total = int(m.group(2).replace(',', '')) if m else 0
                n_orb = int(m_orb.group(1).replace(',', '')) if m_orb else 0
                return done, total, n_orb, line
        except:
            pass
        return 0, 0, 0, ""

    # Poll until all done
    while procs:
        if stop_event and stop_event.is_set():
            LOG.add("task10gb", "STOP requested", level="warning")
            for proc in procs.values():
                proc.terminate()
            for lf in logs.values():
                lf.close()
            return {"status": "paused"}

        time.sleep(5)

        # Handle finished workers (immediate)
        done_keys = [k for k, p in procs.items() if p.poll() is not None]
        for k in done_keys:
            rc = procs[k].returncode
            logs[k].close()
            del procs[k]
            del logs[k]
            pid, sub = k
            if rc == 0:
                cp = CheckpointManager(f"task10gb_p{pid}{sub}").load()
                n_orb = cp.get("n_orbits", "?") if cp else "?"
                LOG.add("task10gb", f"P{pid}{sub} finished ✓ ({n_orb} orbits)", level="success")
            else:
                LOG.add("task10gb", f"P{pid}{sub} CRASHED (exit {rc})", level="error")
                try:
                    log_path = Path(__file__).parent / f"log_10gb_p{pid}{sub}.txt"
                    with open(log_path) as f:
                        for line in f.readlines()[-3:]:
                            LOG.add("task10gb", f"  {line.rstrip()}", level="error")
                except:
                    pass

        # Summary every ~15s
        now = time.time()
        if now - last_summary_time >= 15:
            last_summary_time = now
            sum_done = 0
            sum_total = 0
            sum_orbits = 0
            active = len(procs)

            for pid, sub in sub_parts:
                if (pid, sub) in procs:
                    d, t, o, _ = _parse_10gb_log(pid, sub)
                    sum_done += d
                    sum_total += t
                    sum_orbits += o
                else:
                    cp = CheckpointManager(f"task10gb_p{pid}{sub}").load()
                    if cp and cp.get("status") == "done":
                        sum_done += cp.get("n_sample", 0)
                        sum_total += cp.get("n_sample", 0)
                        sum_orbits += cp.get("n_orbits", 0)

            n_finished = n_total - active
            elapsed = now - t0
            pct = round(100 * sum_done / max(1, sum_total)) if sum_total else 0
            LOG.add("task10gb",
                     f"{sum_done:,}/{sum_total:,} grids ({pct}%) | {sum_orbits:,} orbits | "
                     f"{active} workers | {n_finished}/{n_total} finished | {elapsed:.0f}s",
                     level="info")

        n_done = n_total - len(procs)
        elapsed = time.time() - t0
        if progress_callback:
            pct = 5 + int(80 * n_done / max(1, n_total))
            progress_callback({"phase": "canonicalize", "percent": pct,
                               "message": f"Canon {n_done}/{n_total} done"})

    # Merge all orbit results
    LOG.add("task10gb", "Merging orbit results...", level="info")
    all_orbit_counts = {}
    all_orbit_stabs = {}
    all_orbit_sd = {}
    total_sample = 0
    total_errors = 0

    for pid, sub in sub_parts:
        cp = CheckpointManager(f"task10gb_p{pid}{sub}").load()
        if not cp or cp.get("status") != "done":
            LOG.add("task10gb", f"  P{pid}{sub}: missing result!", level="error")
            continue
        total_sample += cp.get("n_sample", 0)
        total_errors += cp.get("errors", 0)
        for o in cp.get("orbits", []):
            h = o["canon_hash"]
            if h not in all_orbit_counts:
                all_orbit_counts[h] = 0
                all_orbit_stabs[h] = o["stab_structural"]
                all_orbit_sd[h] = o["self_dual"]
            all_orbit_counts[h] += o["sample_count"]

    n_orbits = len(all_orbit_counts)
    self_dual_count = sum(1 for sd in all_orbit_sd.values() if sd)

    # Build stab distributions
    stab_dist_struct = {}
    stab_dist_total = {}
    for h in all_orbit_counts:
        ss = all_orbit_stabs[h]
        sd = all_orbit_sd.get(h, False)
        st = ss * 2 if sd else ss
        stab_dist_struct[ss] = stab_dist_struct.get(ss, 0) + 1
        stab_dist_total[st] = stab_dist_total.get(st, 0) + 1

    top_stab = max(stab_dist_total.keys()) if stab_dist_total else 0
    elapsed = round(time.time() - t0, 1)

    # Build orbits list
    orbits_list = []
    for h in sorted(all_orbit_counts, key=lambda x: -all_orbit_stabs[x]):
        ss = all_orbit_stabs[h]
        sd = all_orbit_sd.get(h, False)
        st = ss * 2 if sd else ss
        orbits_list.append({
            "canon_hash": h, "stab_structural": ss, "stab_total": st,
            "self_dual": sd, "sample_count": all_orbit_counts[h],
            "orbit_size": TOTAL_STRUCTURAL // ss,
        })

    # Get source totals from merge
    src_merge = CheckpointManager("task10g_merged").load()
    n_mates_total = src_merge.get("n_mates_total", 0) if src_merge else 0
    is_exact = src_merge.get("is_exact", False) if src_merge else False

    LOG.add("task10gb", "=" * 60, level="success")
    LOG.add("task10gb", f"DONE in {elapsed:.0f}s ({elapsed/60:.1f} min)", level="success")
    LOG.add("task10gb", f"{total_sample:,} grids → {n_orbits:,} orbits", level="success")
    LOG.add("task10gb", f"Top |Stab| total = {top_stab}", level="success")
    LOG.add("task10gb", f"Self-dual: {self_dual_count}/{n_orbits:,}", level="math")
    LOG.add("task10gb", "Stab distribution (total):", level="math")
    for s in sorted(stab_dist_total, reverse=True):
        LOG.add("task10gb", f"  |Stab|={s}: {stab_dist_total[s]} orbits", level="info")

    result = {
        "status": "done", "source": "back_circulant", "bias": "order3_cycle",
        "method": "parallel_16_workers",
        "n_mates_total": n_mates_total, "is_exact": is_exact,
        "n_sample": total_sample, "n_orbits": n_orbits,
        "self_dual_count": self_dual_count, "top_stab": top_stab,
        "stab_dist_structural": stab_dist_struct,
        "stab_dist_total": stab_dist_total,
        "orbits": orbits_list, "elapsed": elapsed,
        "errors": total_errors,
    }
    ckpt.save(result)

    try:
        export_path = EXPORT_DIR / "task10gb_order3_analysis.json"
        with open(export_path, "w") as f:
            json.dump(result, f, indent=2, default=str)
        LOG.add("task10gb", f"Export -> {export_path}", level="info")
    except Exception as e:
        LOG.add("task10gb", f"Export error: {e}", level="error")

    if progress_callback:
        progress_callback({"percent": 100, "phase": "done",
                           "message": f"Done: {n_orbits:,} orbits from {total_sample:,} grids"})
    return result


# =============================================================
# Task 10H — FUSION: Union of 10a ∪ 10E ∪ 10F ∪ 10G
# =============================================================

def task10h_fusion_analysis(progress_callback=None, stop_event=None):
    """Task 10H — Fuse samples from 10a + 10E + 10F + 10G.
    Loads up to 400k grids (100k per bias), deduplicates by canon hash,
    produces unified orbit/stab/self-dual analysis covering ALL stab>1 mates.
    """
    ckpt = CheckpointManager("task10h_fusion")
    saved = ckpt.load()
    if saved and saved.get("status") == "done":
        return saved

    if not canon_lib_available():
        msg = "canon.dll/.so introuvable."
        LOG.add("task10h", msg, level="error")
        if progress_callback:
            progress_callback({"percent": 0, "message": msg})
        return {"status": "error", "message": msg}

    SOURCES = [
        ("task10a_back_circulant", "r180 (C2)"),
        ("task10e_c3_mates",       "C3 involution"),
        ("task10f_c4_mates",       "C4 transpose"),
        ("task10g_order3_mates",   "order-3 cycle"),
    ]

    all_grids = []
    source_stats = {}

    for ckpt_name, label in SOURCES:
        src_ckpt = CheckpointManager(ckpt_name)
        src = src_ckpt.load()
        if src and src.get("status") == "done":
            raw_sample = src_ckpt.load_sample()
            if raw_sample:
                grids = [_deserialize_grid(g) for g in raw_sample]
                n = len(grids)
                all_grids.extend((g, label) for g in grids)
                source_stats[label] = {
                    "n_sample": n,
                    "n_mates_total": src.get("n_mates_total", 0),
                    "is_exact": src.get("is_exact", False),
                }
                LOG.add("task10h", f"  Loaded {n:,} grids from {label}", level="info")
            else:
                LOG.add("task10h", f"  {label}: no sample data found", level="warning")
        else:
            LOG.add("task10h", f"  {label}: not available (skipped)", level="warning")

    if not all_grids:
        msg = "No source tasks completed. Run 10a/10E/10F/10G first."
        LOG.add("task10h", msg, level="error")
        if progress_callback:
            progress_callback({"percent": 0, "message": msg})
        return {"status": "error", "message": msg}

    n_total_grids = len(all_grids)

    LOG.add("task10h", "=" * 60, level="success")
    LOG.add("task10h", "TASK 10H — FUSION 10a ∪ 10E ∪ 10F ∪ 10G", level="success")
    LOG.add("task10h", "=" * 60, level="success")
    LOG.add("task10h", f"Total: {n_total_grids:,} grids from {len(source_stats)} sources", level="info")

    t0 = time.time()

    start_idx = 0
    orbit_counts = {}
    orbit_stabs = {}
    orbit_grids = {}
    orbit_sources = {}
    if saved and saved.get("status") == "running":
        start_idx = saved.get("processed", 0)
        orbit_counts = saved.get("orbit_counts", {})
        orbit_stabs = saved.get("orbit_stabs", {})
        orbit_sources = {h: set(v) for h, v in saved.get("orbit_sources_list", {}).items()}
        LOG.add("task10h", f"Resuming from grid {start_idx:,}", level="info")

    LOG.add("task10h", f"[1/2] Canon + stab ({n_total_grids:,} grids)...", level="info")
    if progress_callback:
        progress_callback({"phase": "canonicalize", "percent": 1})

    errors = 0
    last_save = time.time()

    for i in range(start_idx, n_total_grids):
        if stop_event and stop_event.is_set():
            LOG.add("task10h", f"STOPPED at {i:,}/{n_total_grids:,}", level="warning")
            ckpt.save({
                "status": "running", "processed": i,
                "orbit_counts": orbit_counts, "orbit_stabs": orbit_stabs,
                "orbit_sources_list": {h: list(v) for h, v in orbit_sources.items()},
            })
            return {"status": "paused"}

        grid, src_label = all_grids[i]
        try:
            h, _, stab = fast_canonicalize_and_stab(grid)
        except Exception as e:
            errors += 1
            if errors <= 5:
                LOG.add("task10h", f"Canon error #{errors}: {e}", level="error")
            continue

        if h not in orbit_counts:
            orbit_counts[h] = 0
            orbit_stabs[h] = stab
            orbit_grids[h] = grid
            orbit_sources[h] = set()
        orbit_counts[h] += 1
        orbit_sources[h].add(src_label)

        done = i + 1
        if done % 5000 == 0 or done == n_total_grids:
            elapsed = time.time() - t0
            rate = (done - start_idx) / max(0.1, elapsed)
            eta = (n_total_grids - done) / max(1, rate)
            n_orb = len(orbit_counts)
            top_s = max(orbit_stabs.values()) if orbit_stabs else 0
            LOG.add("task10h",
                     f"  {done:,}/{n_total_grids:,} — {n_orb:,} orbits, top|Stab|={top_s} "
                     f"({rate:.1f}/s, ETA {eta:.0f}s)", level="info")
            if progress_callback:
                pct = 5 + int(85 * done / n_total_grids)
                progress_callback({"phase": "canonicalize", "percent": pct,
                                   "message": f"Canon {done:,}/{n_total_grids:,} — {n_orb:,} orbits",
                                   "n_orbits": n_orb})

        if time.time() - last_save > 120:
            ckpt.save({
                "status": "running", "processed": done,
                "orbit_counts": orbit_counts, "orbit_stabs": orbit_stabs,
                "orbit_sources_list": {h: list(v) for h, v in orbit_sources.items()},
            })
            last_save = time.time()

    n_orbits = len(orbit_counts)
    LOG.add("task10h", f"[1/2] Done: {n_orbits:,} orbits ({errors} errors)", level="success")

    LOG.add("task10h", f"[2/2] Swap check ({n_orbits:,} orbits)...", level="info")
    self_dual_count = 0
    orbit_sd = {}
    for h in orbit_grids:
        grid = orbit_grids[h]
        if grid is None:
            orbit_sd[h] = False
            continue
        swapped = tuple(tuple((cell[1], cell[0]) for cell in row) for row in grid)
        try:
            h_swap, _, _ = fast_canonicalize_and_stab(swapped)
            orbit_sd[h] = (h_swap == h)
            if h_swap == h:
                self_dual_count += 1
        except:
            orbit_sd[h] = False

    LOG.add("task10h", f"[2/2] Self-dual: {self_dual_count}/{n_orbits:,}", level="math")

    stab_dist_struct = {}
    stab_dist_total = {}
    for h in orbit_counts:
        ss = orbit_stabs[h]
        st = ss * 2 if orbit_sd.get(h, False) else ss
        stab_dist_struct[ss] = stab_dist_struct.get(ss, 0) + 1
        stab_dist_total[st] = stab_dist_total.get(st, 0) + 1

    top_stab = max(stab_dist_total.keys()) if stab_dist_total else 0
    elapsed = round(time.time() - t0, 1)

    multi_source = sum(1 for h in orbit_sources if len(orbit_sources[h]) > 1)
    exclusive = {}
    for label in source_stats:
        exclusive[label] = sum(1 for h in orbit_sources if orbit_sources[h] == {label})

    orbits_list = []
    for h in sorted(orbit_counts, key=lambda x: -orbit_stabs[x]):
        ss = orbit_stabs[h]
        sd = orbit_sd.get(h, False)
        st = ss * 2 if sd else ss
        orbits_list.append({
            "canon_hash": h, "stab_structural": ss, "stab_total": st,
            "self_dual": sd, "sample_count": orbit_counts[h],
            "orbit_size": TOTAL_STRUCTURAL // ss,
            "sources": sorted(orbit_sources.get(h, set())),
        })

    LOG.add("task10h", "=" * 60, level="success")
    LOG.add("task10h", f"FUSION DONE in {elapsed:.0f}s ({elapsed/60:.1f} min)", level="success")
    LOG.add("task10h", f"{n_total_grids:,} grids → {n_orbits:,} unique orbits", level="success")
    LOG.add("task10h", f"Top |Stab| total = {top_stab}", level="success")
    LOG.add("task10h", f"Self-dual: {self_dual_count}/{n_orbits:,}", level="math")
    LOG.add("task10h", f"Orbits in >1 source: {multi_source}", level="math")
    for label in sorted(exclusive):
        LOG.add("task10h", f"  Exclusive to {label}: {exclusive[label]} orbits", level="info")
    LOG.add("task10h", "Stab distribution (total):", level="math")
    for s in sorted(stab_dist_total, reverse=True):
        LOG.add("task10h", f"  |Stab|={s}: {stab_dist_total[s]} orbits", level="info")

    result = {
        "status": "done", "source": "fusion_10a_10E_10F_10G",
        "n_grids_analyzed": n_total_grids,
        "source_stats": source_stats,
        "n_orbits": n_orbits,
        "self_dual_count": self_dual_count, "top_stab": top_stab,
        "multi_source_orbits": multi_source,
        "exclusive_orbits": exclusive,
        "stab_dist_structural": stab_dist_struct,
        "stab_dist_total": stab_dist_total,
        "orbits": orbits_list,
        "elapsed": elapsed,
    }
    ckpt.save(result)

    try:
        export_path = EXPORT_DIR / "task10h_fusion.json"
        export_path.parent.mkdir(parents=True, exist_ok=True)
        with open(export_path, "w") as f:
            json.dump(result, f, indent=2, default=str)
        LOG.add("task10h", f"Export -> {export_path}", level="info")
    except Exception as e:
        LOG.add("task10h", f"Export error: {e}", level="error")

    if progress_callback:
        progress_callback({"percent": 100, "phase": "done",
                           "message": f"Done: {n_orbits:,} orbits from {len(source_stats)} sources"})
    return result


# =============================================================
# Task 10I — Unbiased mates (exhaustive, no symmetry constraint)
# =============================================================

def task10i_unbiased_mates(progress_callback=None, stop_event=None):
    """Task 10I — Count ALL color mates without any symmetry bias.
    Single-process version. For partitioned version, use launch_10i.py."""
    ckpt = CheckpointManager("task10i_unbiased_mates")
    saved = ckpt.load()
    if saved and saved.get("status") == "done":
        return saved

    LOG.add("task10i", "=" * 60, level="success")
    LOG.add("task10i", "TASK 10I — ALL MATES (UNBIASED, NO SYMMETRY CONSTRAINT)", level="success")
    LOG.add("task10i", "=" * 60, level="success")
    LOG.add("task10i", "No symmetry bias — explores full solution space", level="info")
    LOG.add("task10i", "Breaking: color[0][0]=0 → result × 9 = exact total", level="math")

    start = time.time()
    if progress_callback:
        progress_callback({"phase": "solving", "percent": 5, "message": "CP-SAT solving (unbiased)..."})

    def _on_mate_found(n, elapsed):
        rate = n / max(1, elapsed)
        if n <= 100 or n % 1_000_000 == 0:
            LOG.add("task10i", f"  {n:,} mates ({elapsed:.0f}s, {rate:.0f}/s)", level="info")
        if progress_callback:
            progress_callback({
                "phase": "solving",
                "percent": min(95, 5 + int(elapsed / 120)),
                "message": f"Counting... {n:,} mates ({elapsed:.0f}s, {rate:.0f}/s)",
                "found": n,
            })
        if n % 100000 == 0:
            ckpt.save({"status": "running", "n_breaking_so_far": n, "elapsed_so_far": elapsed})

    mates_sample, n_breaking, status = _find_all_color_mates(
        BACK_CIRCULANT, exhaustive=True, timeout=86400,
        on_solution_found=_on_mate_found, sample_size=100000,
        use_rot180=False, symmetry_fn=None)

    from ortools.sat.python import cp_model as cpm
    elapsed = round(time.time() - start, 1)
    is_exact = status in (cpm.OPTIMAL, cpm.FEASIBLE) and elapsed < 86000
    n_total = n_breaking * 9

    LOG.add("task10i", f"RESULT: {n_breaking:,} breaking × 9 = {n_total:,} total", level="success")
    LOG.add("task10i", f"Status: {'EXACT' if is_exact else 'INCOMPLETE'}", level="success" if is_exact else "warning")
    LOG.add("task10i", f"Sample: {len(mates_sample)} grids | Time: {elapsed}s", level="info")

    result = {
        "status": "done", "source": "back_circulant", "bias": "none",
        "n_mates_breaking": n_breaking, "n_mates_total": n_total,
        "is_exact": is_exact, "n_sample": len(mates_sample),
        "sample": [_serialize_grid(m) for m in mates_sample], "elapsed": elapsed,
    }
    ckpt.save(result)
    try:
        with open(EXPORT_DIR / "task10i_unbiased_mates.json", "w") as f:
            json.dump({"description": "Unbiased mates for BC (no symmetry constraint)",
                        "bias": "none",
                        "n_mates_breaking": n_breaking, "n_mates_total": n_total,
                        "is_exact": is_exact, "n_sample": len(mates_sample),
                        "elapsed": elapsed, "timestamp": datetime.now().isoformat()}, f, indent=2, default=str)
    except Exception as e:
        LOG.add("task10i", f"Export error: {e}", level="error")

    if progress_callback:
        progress_callback({"percent": 100, "phase": "done"})
    return result


# =============================================================
# Task 10I Partitioned — 8×2 parallel workers (subprocess)
# =============================================================

def _task10i_partition(part_id, progress_callback=None, stop_event=None):
    """Task 10I partition: launches 2 worker subprocesses (a+b) per partition.
    True parallelism — each worker has its own Python process + GIL.
    Uses 2 cores per partition = 16 cores total for 8 partitions."""
    import subprocess

    task_label = f"task10i_p{part_id}"
    ckpt_a_name = f"task10i_p{part_id}a_unbiased"
    ckpt_b_name = f"task10i_p{part_id}b_unbiased"
    ckpt_a = CheckpointManager(ckpt_a_name)
    ckpt_b = CheckpointManager(ckpt_b_name)

    # Check if both already done
    saved_a = ckpt_a.load()
    saved_b = ckpt_b.load()
    if (saved_a and saved_a.get("status") == "done" and
        saved_b and saved_b.get("status") == "done"):
        total = saved_a["n_mates_breaking"] + saved_b["n_mates_breaking"]
        return {
            "status": "done", "partition": part_id,
            "n_mates_breaking": total,
            "is_exact": saved_a.get("is_exact", False) and saved_b.get("is_exact", False),
            "n_sample": saved_a.get("n_sample", 0) + saved_b.get("n_sample", 0),
            "elapsed": max(saved_a.get("elapsed", 0), saved_b.get("elapsed", 0)),
        }

    LOG.add(task_label, "=" * 60, level="success")
    LOG.add(task_label, f"TASK 10I-P{part_id} — 2 SUBPROCESSES (a+b)", level="success")
    LOG.add(task_label, "=" * 60, level="success")

    # Find worker script
    import sys
    worker_path = Path(__file__).parent / "worker_10i.py"
    if not worker_path.exists():
        msg = f"worker_10i.py not found at {worker_path}"
        LOG.add(task_label, msg, level="error")
        if progress_callback:
            progress_callback({"percent": 0, "message": msg})
        return {"status": "error", "message": msg}

    # Launch 2 subprocesses
    procs = {}
    logs = {}
    for sub in ('a', 'b'):
        ckpt_sub = ckpt_a if sub == 'a' else ckpt_b
        saved_sub = ckpt_sub.load()
        if saved_sub and saved_sub.get("status") == "done":
            LOG.add(task_label, f"  P{part_id}{sub} already done, skipping", level="info")
            continue
        log_path = Path(__file__).parent / f"log_10i_p{part_id}{sub}.txt"
        log_file = open(log_path, "w")
        proc = subprocess.Popen(
            [sys.executable, str(worker_path), str(part_id), sub],
            stdout=log_file, stderr=subprocess.STDOUT,
            cwd=str(Path(__file__).parent)
        )
        procs[sub] = proc
        logs[sub] = log_file
        LOG.add(task_label, f"  P{part_id}{sub} PID={proc.pid}", level="info")

    if progress_callback:
        n_procs = len(procs)
        progress_callback({"phase": "solving", "percent": 5,
                           "message": f"P{part_id}: {n_procs} subprocess(es) running"})

    # Poll checkpoints until all done
    start = time.time()
    while procs:
        if stop_event and stop_event.is_set():
            LOG.add(task_label, f"STOP requested, killing subprocesses", level="warning")
            for sub, proc in procs.items():
                proc.terminate()
                try:
                    proc.wait(timeout=10)
                except:
                    proc.kill()
            for lf in logs.values():
                lf.close()
            return {"status": "paused"}

        time.sleep(3)

        # Check which procs finished
        done_subs = []
        for sub, proc in procs.items():
            if proc.poll() is not None:
                done_subs.append(sub)
        for sub in done_subs:
            logs[sub].close()
            del procs[sub]
            del logs[sub]
            LOG.add(task_label, f"  P{part_id}{sub} subprocess finished", level="info")

        # Read checkpoints for combined progress
        total_so_far = 0
        details = []
        for sub in ('a', 'b'):
            ckpt_sub = CheckpointManager(f"task10i_p{part_id}{sub}_unbiased")
            sp = ckpt_sub.load()
            if sp:
                if sp.get("status") == "done":
                    total_so_far += sp["n_mates_breaking"]
                    details.append(f"{sub}:DONE({sp['n_mates_breaking']:,})")
                elif sp.get("status") == "running":
                    n = sp.get("n_breaking_so_far", 0)
                    total_so_far += n
                    details.append(f"{sub}:{n:,}")
            else:
                details.append(f"{sub}:?")

        elapsed = time.time() - start
        LOG.add(task_label, f"  [{elapsed:.0f}s] {' | '.join(details)} — total so far: {total_so_far:,}", level="info")

        if progress_callback:
            pct = min(95, 5 + int(elapsed / 60))
            progress_callback({
                "phase": "solving", "percent": pct,
                "message": f"P{part_id}: {total_so_far:,} breaking ({elapsed:.0f}s)",
                "found": total_so_far,
            })

    # Done — combine
    saved_a = ckpt_a.load()
    saved_b = ckpt_b.load()
    total_breaking = 0
    if saved_a and saved_a.get("status") == "done":
        total_breaking += saved_a["n_mates_breaking"]
    if saved_b and saved_b.get("status") == "done":
        total_breaking += saved_b["n_mates_breaking"]

    combined = {
        "status": "done", "partition": part_id,
        "bias": "none",
        "n_mates_breaking": total_breaking,
        "is_exact": (saved_a or {}).get("is_exact", False) and (saved_b or {}).get("is_exact", False),
        "n_sample": (saved_a or {}).get("n_sample", 0) + (saved_b or {}).get("n_sample", 0),
        "elapsed": round(time.time() - start, 1),
    }

    LOG.add(task_label, f"P{part_id} DONE: {total_breaking:,} breaking", level="success")
    if progress_callback:
        progress_callback({"percent": 100, "phase": "done",
                           "message": f"P{part_id} done: {total_breaking:,} breaking"})
    return combined


# Factory: create the 8 task functions for 10I
def _make_task10i_part(pid):
    def task_fn(progress_callback=None, stop_event=None):
        return _task10i_partition(pid, progress_callback, stop_event)
    task_fn.__name__ = f"task10i_p{pid}_unbiased_mates"
    return task_fn

task10i_p1 = _make_task10i_part(1)
task10i_p2 = _make_task10i_part(2)
task10i_p3 = _make_task10i_part(3)
task10i_p4 = _make_task10i_part(4)
task10i_p5 = _make_task10i_part(5)
task10i_p6 = _make_task10i_part(6)
task10i_p7 = _make_task10i_part(7)
task10i_p8 = _make_task10i_part(8)


def task10i_merge(progress_callback=None, stop_event=None):
    """Merge all 8×2 partitions of Task 10I into a single result."""
    ckpt = CheckpointManager("task10i_merged")
    saved = ckpt.load()
    if saved and saved.get("status") == "done":
        return saved

    LOG.add("task10i_merge", "=" * 60, level="success")
    LOG.add("task10i_merge", "TASK 10I MERGE — Combining 8×2 partitions (unbiased)", level="success")
    LOG.add("task10i_merge", "=" * 60, level="success")

    total_breaking = 0
    all_exact = True
    total_elapsed = 0
    partition_results = {}
    all_samples = []

    for pid in range(1, 9):
        part_total = 0
        part_exact = True
        part_elapsed = 0

        for sub in ('a', 'b'):
            ckpt_name = f"task10i_p{pid}{sub}_unbiased"
            ckpt_p = CheckpointManager(ckpt_name)
            sp = ckpt_p.load()
            if not sp:
                LOG.add("task10i_merge", f"  P{pid}{sub}: NOT FOUND", level="error")
                part_exact = False
                all_exact = False
                continue
            if sp.get("status") == "running":
                n = sp.get("n_breaking_so_far", 0)
                LOG.add("task10i_merge", f"  P{pid}{sub}: RUNNING ({n:,} so far)", level="warning")
                part_total += n
                part_exact = False
                all_exact = False
                continue
            if sp.get("status") != "done":
                LOG.add("task10i_merge", f"  P{pid}{sub}: status={sp.get('status')}", level="warning")
                part_exact = False
                all_exact = False
                continue

            nb = sp["n_mates_breaking"]
            part_total += nb
            part_elapsed = max(part_elapsed, sp.get("elapsed", 0))
            if not sp.get("is_exact", False):
                part_exact = False
                all_exact = False
            LOG.add("task10i_merge", f"  P{pid}{sub}: {nb:,} breaking ({'exact' if sp.get('is_exact') else 'partial'})", level="info")

            # Collect samples
            sample_file = sp.get("sample_file")
            if sample_file:
                sample_path = CHECKPOINT_DIR / sample_file
                try:
                    with open(sample_path) as sf:
                        sample_data = json.load(sf)
                    all_samples.append((nb, sample_data))
                    LOG.add("task10i_merge", f"    Loaded {len(sample_data):,} grids from {sample_file}", level="info")
                except Exception as e:
                    LOG.add("task10i_merge", f"    Sample file error: {e}", level="error")
            elif "sample" in sp and sp["sample"]:
                all_samples.append((nb, sp["sample"]))

        total_breaking += part_total
        total_elapsed = max(total_elapsed, part_elapsed)
        partition_results[pid] = {"n_breaking": part_total, "is_exact": part_exact}
        LOG.add("task10i_merge", f"  P{pid} combined: {part_total:,} breaking", level="info")

    n_total = total_breaking * 9
    n_partitions_done = sum(1 for v in partition_results.values() if v.get("is_exact", False))

    LOG.add("task10i_merge", f"Partitions done: {n_partitions_done}/8", level="info")
    LOG.add("task10i_merge", f"TOTAL: {total_breaking:,} breaking × 9 = {n_total:,} total", level="success")
    LOG.add("task10i_merge", f"All exact: {all_exact}", level="success" if all_exact else "warning")

    # Concatenate ALL samples
    merged_sample = []
    if all_samples:
        for weight, sample_list in all_samples:
            merged_sample.extend(sample_list)
        LOG.add("task10i_merge", f"Total sample: {len(merged_sample):,} grids (all kept)", level="info")

    result = {
        "status": "done" if all_exact else "partial",
        "source": "back_circulant", "bias": "none",
        "method": "partitioned_8x2",
        "n_mates_breaking": total_breaking,
        "n_mates_total": n_total,
        "is_exact": all_exact,
        "n_partitions_done": n_partitions_done,
        "partition_results": partition_results,
        "n_sample": len(merged_sample),
        "elapsed_max": total_elapsed,
    }
    ckpt.save(result)

    # Save compat checkpoint for 10Ib
    if all_exact:
        compat_result = {
            "status": "done", "source": "back_circulant", "bias": "none",
            "n_mates_breaking": total_breaking, "n_mates_total": n_total,
            "is_exact": True, "n_sample": len(merged_sample), "elapsed": total_elapsed,
        }
        CheckpointManager("task10i_unbiased_mates").save(compat_result)
        LOG.add("task10i_merge", "Saved compat checkpoint → task10i_unbiased_mates", level="info")

    try:
        with open(EXPORT_DIR / "task10i_merged.json", "w") as f:
            json.dump({"description": "Rot180-only mates (partitioned merge)",
                        "n_mates_breaking": total_breaking, "n_mates_total": n_total,
                        "is_exact": all_exact, "n_sample": len(merged_sample),
                        "n_partitions_done": n_partitions_done,
                        "partition_results": partition_results,
                        "elapsed_max": total_elapsed,
                        "timestamp": datetime.now().isoformat()}, f, indent=2, default=str)
    except Exception as e:
        LOG.add("task10i_merge", f"Export error: {e}", level="error")

    if progress_callback:
        progress_callback({"percent": 100, "phase": "done",
                           "message": f"Merged: {total_breaking:,} × 9 = {n_total:,}"})
    return result


# =============================================================
# Task 10Ib — Unbiased orbit analysis (parallel: 16 subprocesses)
# =============================================================

def task10ib_unbiased_analysis(progress_callback=None, stop_event=None):
    """Task 10Ib — Analyze unbiased sample (parallel: 16 subprocesses).
    Each subprocess canonicalizes one sub-partition's 100k sample.
    Results are merged into unified orbit analysis."""
    import subprocess
    import sys
    import re

    ckpt = CheckpointManager("task10ib_unbiased_analysis")
    saved = ckpt.load()
    if saved and saved.get("status") == "done":
        return saved

    if not canon_lib_available():
        msg = "canon.dll/.so introuvable."
        LOG.add("task10ib", msg, level="error")
        if progress_callback:
            progress_callback({"percent": 0, "message": msg})
        return {"status": "error", "message": msg}

    worker_path = Path(__file__).parent / "worker_10ib.py"
    if not worker_path.exists():
        msg = f"worker_10ib.py not found"
        LOG.add("task10ib", msg, level="error")
        if progress_callback:
            progress_callback({"percent": 0, "message": msg})
        return {"status": "error", "message": msg}

    # Collect which sub-partitions have data
    sub_parts = []
    for pid in range(1, 9):
        for sub in ('a', 'b'):
            src_ckpt = CheckpointManager(f"task10i_p{pid}{sub}_unbiased")
            src = src_ckpt.load()
            if src and src.get("status") == "done" and (src.get("sample") or src.get("sample_file")):
                sub_parts.append((pid, sub))

    if not sub_parts:
        # Fallback: try unified checkpoint
        src_ckpt = CheckpointManager("task10i_unbiased_mates")
        src = src_ckpt.load()
        if src and src.get("status") == "done" and (src.get("sample") or src.get("sample_file")):
            LOG.add("task10ib", "No sub-partitions found, falling back to sequential", level="warning")
            return _task10x_analyze_sample("task10ib", "task10i_unbiased_mates", "task10ib_unbiased_analysis",
                                            progress_callback, stop_event)
        msg = "No source data found. Run 10I partitions first."
        LOG.add("task10ib", msg, level="error")
        return {"status": "error", "message": msg}

    LOG.add("task10ib", "=" * 60, level="success")
    LOG.add("task10ib", f"TASK 10Ib — CANONICALIZATION ({len(sub_parts)} sub-partitions)", level="success")
    LOG.add("task10ib", "=" * 60, level="success")
    LOG.add("task10ib", f"Found {len(sub_parts)} sub-partitions to analyze", level="info")

    if progress_callback:
        progress_callback({"phase": "launching", "percent": 2,
                           "message": f"Launching {len(sub_parts)} workers..."})

    # Launch all workers at once
    t0 = time.time()
    procs = {}
    logs = {}
    for pid, sub in sub_parts:
        already = CheckpointManager(f"task10ib_p{pid}{sub}").load()
        if already and already.get("status") == "done":
            LOG.add("task10ib", f"  P{pid}{sub} already done, skip", level="info")
            continue
        log_path = Path(__file__).parent / f"log_10ib_p{pid}{sub}.txt"
        lf = open(log_path, "w")
        env = os.environ.copy()
        env["PYTHONUNBUFFERED"] = "1"
        proc = subprocess.Popen(
            [sys.executable, str(worker_path), str(pid), sub],
            stdout=lf, stderr=subprocess.STDOUT,
            cwd=str(Path(__file__).parent), env=env
        )
        procs[(pid, sub)] = proc
        logs[(pid, sub)] = lf

    LOG.add("task10ib", f"Launched {len(procs)} workers", level="info")

    n_total = len(sub_parts)
    last_summary_time = 0

    def _parse_10ib_log(pid, sub):
        """Read worker log, return (done, total, n_orbits, last_line)."""
        try:
            log_path = Path(__file__).parent / f"log_10ib_p{pid}{sub}.txt"
            with open(log_path, "r") as rf:
                lines = rf.readlines()
            for line in reversed(lines):
                line = line.strip()
                if not line:
                    continue
                m = re.search(r'(\d[\d,]*)/(\d[\d,]*)', line)
                m_orb = re.search(r'([\d,]+)\s*orbits', line)
                done = int(m.group(1).replace(',', '')) if m else 0
                total = int(m.group(2).replace(',', '')) if m else 0
                n_orb = int(m_orb.group(1).replace(',', '')) if m_orb else 0
                return done, total, n_orb, line
        except:
            pass
        return 0, 0, 0, ""

    # Poll until all done
    while procs:
        if stop_event and stop_event.is_set():
            LOG.add("task10ib", "STOP requested", level="warning")
            for proc in procs.values():
                proc.terminate()
            for lf in logs.values():
                lf.close()
            return {"status": "paused"}

        time.sleep(5)

        # Handle finished workers
        done_keys = [k for k, p in procs.items() if p.poll() is not None]
        for k in done_keys:
            rc = procs[k].returncode
            logs[k].close()
            del procs[k]
            del logs[k]
            pid, sub = k
            if rc == 0:
                cp = CheckpointManager(f"task10ib_p{pid}{sub}").load()
                n_orb = cp.get("n_orbits", "?") if cp else "?"
                LOG.add("task10ib", f"P{pid}{sub} finished ✓ ({n_orb} orbits)", level="success")
            else:
                LOG.add("task10ib", f"P{pid}{sub} CRASHED (exit {rc})", level="error")
                try:
                    log_path = Path(__file__).parent / f"log_10ib_p{pid}{sub}.txt"
                    with open(log_path) as f:
                        for line in f.readlines()[-3:]:
                            LOG.add("task10ib", f"  {line.rstrip()}", level="error")
                except:
                    pass

        # Summary every ~15s
        now = time.time()
        if now - last_summary_time >= 15:
            last_summary_time = now
            sum_done = 0
            sum_total = 0
            sum_orbits = 0
            active = len(procs)

            for pid, sub in sub_parts:
                if (pid, sub) in procs:
                    d, t, o, _ = _parse_10ib_log(pid, sub)
                    sum_done += d
                    sum_total += t
                    sum_orbits += o
                else:
                    cp = CheckpointManager(f"task10ib_p{pid}{sub}").load()
                    if cp and cp.get("status") == "done":
                        sum_done += cp.get("n_sample", 0)
                        sum_total += cp.get("n_sample", 0)
                        sum_orbits += cp.get("n_orbits", 0)

            n_finished = n_total - active
            elapsed = now - t0
            pct = round(100 * sum_done / max(1, sum_total)) if sum_total else 0
            LOG.add("task10ib",
                     f"{sum_done:,}/{sum_total:,} grids ({pct}%) | {sum_orbits:,} orbits | "
                     f"{active} workers | {n_finished}/{n_total} finished | {elapsed:.0f}s",
                     level="info")

        n_done = n_total - len(procs)
        elapsed = time.time() - t0
        if progress_callback:
            pct = 5 + int(80 * n_done / max(1, n_total))
            progress_callback({"phase": "canonicalize", "percent": pct,
                               "message": f"Canon {n_done}/{n_total} done"})

    # Merge all orbit results
    LOG.add("task10ib", "Merging orbit results...", level="info")
    all_orbit_counts = {}
    all_orbit_stabs = {}
    all_orbit_sd = {}
    total_sample = 0
    total_errors = 0

    for pid, sub in sub_parts:
        cp = CheckpointManager(f"task10ib_p{pid}{sub}").load()
        if not cp or cp.get("status") != "done":
            LOG.add("task10ib", f"  P{pid}{sub}: missing result!", level="error")
            continue
        total_sample += cp.get("n_sample", 0)
        total_errors += cp.get("errors", 0)
        for o in cp.get("orbits", []):
            h = o["canon_hash"]
            if h not in all_orbit_counts:
                all_orbit_counts[h] = 0
                all_orbit_stabs[h] = o["stab_structural"]
                all_orbit_sd[h] = o["self_dual"]
            all_orbit_counts[h] += o["sample_count"]

    n_orbits = len(all_orbit_counts)
    self_dual_count = sum(1 for sd in all_orbit_sd.values() if sd)

    # Build stab distributions
    stab_dist_struct = {}
    stab_dist_total = {}
    for h in all_orbit_counts:
        ss = all_orbit_stabs[h]
        sd = all_orbit_sd.get(h, False)
        st = ss * 2 if sd else ss
        stab_dist_struct[ss] = stab_dist_struct.get(ss, 0) + 1
        stab_dist_total[st] = stab_dist_total.get(st, 0) + 1

    top_stab = max(stab_dist_total.keys()) if stab_dist_total else 0
    elapsed = round(time.time() - t0, 1)

    # Build orbits list
    orbits_list = []
    for h in sorted(all_orbit_counts, key=lambda x: -all_orbit_stabs[x]):
        ss = all_orbit_stabs[h]
        sd = all_orbit_sd.get(h, False)
        st = ss * 2 if sd else ss
        orbits_list.append({
            "canon_hash": h, "stab_structural": ss, "stab_total": st,
            "self_dual": sd, "sample_count": all_orbit_counts[h],
            "orbit_size": TOTAL_STRUCTURAL // ss,
        })

    # Get source totals from merge
    src_merge = CheckpointManager("task10i_merged").load()
    n_mates_total = src_merge.get("n_mates_total", 0) if src_merge else 0
    is_exact = src_merge.get("is_exact", False) if src_merge else False

    LOG.add("task10ib", "=" * 60, level="success")
    LOG.add("task10ib", f"DONE in {elapsed:.0f}s ({elapsed/60:.1f} min)", level="success")
    LOG.add("task10ib", f"{total_sample:,} grids → {n_orbits:,} orbits", level="success")
    LOG.add("task10ib", f"Top |Stab| total = {top_stab}", level="success")
    LOG.add("task10ib", f"Self-dual: {self_dual_count}/{n_orbits:,}", level="math")
    LOG.add("task10ib", "Stab distribution (total):", level="math")
    for s in sorted(stab_dist_total, reverse=True):
        LOG.add("task10ib", f"  |Stab|={s}: {stab_dist_total[s]} orbits", level="info")

    result = {
        "status": "done", "source": "back_circulant", "bias": "none",
        "method": "parallel_16_workers",
        "n_mates_total": n_mates_total, "is_exact": is_exact,
        "n_sample": total_sample, "n_orbits": n_orbits,
        "self_dual_count": self_dual_count, "top_stab": top_stab,
        "stab_dist_structural": stab_dist_struct,
        "stab_dist_total": stab_dist_total,
        "orbits": orbits_list, "elapsed": elapsed,
        "errors": total_errors,
    }
    ckpt.save(result)

    try:
        export_path = EXPORT_DIR / "task10ib_unbiased_analysis.json"
        with open(export_path, "w") as f:
            json.dump(result, f, indent=2, default=str)
        LOG.add("task10ib", f"Export -> {export_path}", level="info")
    except Exception as e:
        LOG.add("task10ib", f"Export error: {e}", level="error")

    if progress_callback:
        progress_callback({"percent": 100, "phase": "done",
                           "message": f"Done: {n_orbits:,} orbits from {total_sample:,} grids"})
    return result

