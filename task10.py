#!/usr/bin/env python3
"""
Task 10 — Back-circulant: Generation (16 workers = 8 partitions × 2 sub a/b) + Catalogue (8 workers)
color[0][0]=0, color[0][1]=part_id, color[0][2] sub-partitioned.
Result × 9 = exact total.

PROBE SYSTEM: choose symmetry constraint for generation phase.
  - standard : no symmetry constraint (full exhaustive)
  - r180     : rot180 color symmetry (halves search space)
  - order3   : order-3 column cycle symmetry
  - c3inv    : C3 involution symmetry
  - c4trans  : C4 transpose symmetry
"""

import time
import json
import threading
import os
from pathlib import Path

from engine import (
    _find_all_color_mates, _serialize_grid, _deserialize_grid,
    fast_canonicalize_and_stab, canon_lib_available,
    add_rot180_color_only, add_C3_involution_color_only,
    add_C4_transpose_color_only, add_order3_cycle_color_only,
    BACK_CIRCULANT, CheckpointManager, LOG, EXPORT_DIR, TOTAL_STRUCTURAL,
)

DIGIT_GRID = BACK_CIRCULANT  # 1-9 format

# ─── Probe type registry (shared with task13) ───
PROBE_TYPES = {
    "standard": {
        "label": "Standard (aucune symétrie)",
        "use_rot180": False,
        "symmetry_fn": None,
        "multiplier": 9,
        "description": "Énumération exhaustive sans contrainte de symétrie sur la couche couleur",
    },
    "r180": {
        "label": "Rotation 180°",
        "use_rot180": True,
        "symmetry_fn": None,
        "multiplier": 9,
        "description": "Seuls les mates dont la couche couleur a la symétrie rot180. "
                       "Espace de recherche réduit ×2 (cellules couplées (r,c)↔(8-r,8-c)).",
    },
    "order3": {
        "label": "Ordre 3 (cycle colonnes)",
        "use_rot180": False,
        "symmetry_fn": add_order3_cycle_color_only,
        "multiplier": 9,
        "description": "Cycle d'ordre 3 sur les colonnes : 0→1→2→0 dans chaque stack.",
    },
    "c3inv": {
        "label": "C3 involution",
        "use_rot180": False,
        "symmetry_fn": add_C3_involution_color_only,
        "multiplier": 9,
        "description": "Involution d'ordre 2 (classe C3), 18 éléments, 9 cellules fixes.",
    },
    "c4trans": {
        "label": "C4 transposée",
        "use_rot180": False,
        "symmetry_fn": add_C4_transpose_color_only,
        "multiplier": 9,
        "description": "Involution transposée d'ordre 2 (classe C4), 18 éléments.",
    },
}


def _ckpt_suffix(probe_type):
    if probe_type == "standard":
        return "bc"
    return f"bc_{probe_type}"

def _ckpt_name(worker_id, probe_type):
    return f"task10_g{worker_id}_{_ckpt_suffix(probe_type)}"

def _sample_filename(worker_id, probe_type):
    return f"task10_g{worker_id}_{_ckpt_suffix(probe_type)}_sample.json"

def _catalog_path(probe_type):
    if probe_type == "standard":
        return EXPORT_DIR / "task10_catalog.json"
    return EXPORT_DIR / f"task10_catalog_{probe_type}.json"


class PerfLogger:
    def __init__(self, filepath):
        self.filepath = Path(filepath)
        self.filepath.parent.mkdir(parents=True, exist_ok=True)
        self.entries = []
        self._last_write = 0
        self._last_n = 0

    def _interval(self, elapsed_s):
        if elapsed_s < 15 * 60: return 60
        elif elapsed_s < 60 * 60: return 5 * 60
        else: return 15 * 60

    def maybe_log(self, n_found, elapsed_s):
        if elapsed_s - self._last_write < self._interval(elapsed_s): return
        dt = elapsed_s - self._last_write if self._last_write > 0 else elapsed_s
        dn = n_found - self._last_n
        self.entries.append({
            "t": round(elapsed_s, 1), "n": n_found,
            "rate": round(n_found / max(0.1, elapsed_s), 1),
            "rate_interval": round(dn / max(0.1, dt), 1),
        })
        self._last_write = elapsed_s
        self._last_n = n_found
        self._flush()

    def finalize(self, n_found, elapsed_s):
        self.entries.append({
            "t": round(elapsed_s, 1), "n": n_found,
            "rate": round(n_found / max(0.1, elapsed_s), 1),
            "rate_interval": 0, "final": True,
        })
        self._flush()

    def _flush(self):
        try:
            tmp = self.filepath.with_suffix('.tmp')
            with open(tmp, 'w') as f: json.dump(self.entries, f, indent=1)
            tmp.replace(self.filepath)
        except Exception: pass


def _worker_to_partition(worker_id):
    part_id = (worker_id - 1) // 2 + 1
    sub = 'a' if worker_id % 2 == 1 else 'b'
    return part_id, sub

def _get_sub_partition_values(part_id, sub):
    available = sorted([v for v in range(1, 9) if v != part_id])
    mid = len(available) // 2
    if sub == 'a': return available[:mid + 1]
    else: return available[mid + 1:]


# =============================================================
# Phase 1: Generation workers (G1-G16) — with probe type
# =============================================================

def task10_generate_worker(worker_id, n_target=0,
                           progress_callback=None, stop_event=None,
                           probe_type="standard"):
    if probe_type not in PROBE_TYPES:
        LOG.add("task10", f"[G{worker_id}] Unknown probe type: {probe_type}, falling back to standard", level="warning")
        probe_type = "standard"

    probe = PROBE_TYPES[probe_type]
    part_id, sub = _worker_to_partition(worker_id)
    label = f"G{worker_id}/{probe_type}"
    p2_values = _get_sub_partition_values(part_id, sub)

    ckpt_n = _ckpt_name(worker_id, probe_type)
    ckpt = CheckpointManager(ckpt_n)
    saved = ckpt.load()
    sample_fname = _sample_filename(worker_id, probe_type)
    sample_path = Path(ckpt.filepath).parent / sample_fname

    if saved and saved.get("status") == "done":
        n = saved.get("n_mates_breaking", 0)
        LOG.add("task10", f"[{label}] Already done: {n:,} breaking mates", level="success")
        return {"status": "done", "n_found": n * probe["multiplier"],
                "n_breaking": n, "n_target": n_target,
                "partition": part_id, "sub": sub, "probe_type": probe_type}

    LOG.add("task10", f"[{label}] START — partition={part_id}, sub={sub}, "
            f"probe={probe_type} ({probe['label']}), "
            f"color[0][1]={part_id}, color[0][2] in {p2_values}", level="success")

    mult = probe["multiplier"]
    t0 = time.time()
    last_cb = [time.time()]
    _coll_ref = [None]
    _last_sample_save = [0]

    perf_path = Path(ckpt.filepath).parent / f"task10_g{worker_id}_{_ckpt_suffix(probe_type)}_perf.json"
    perf = PerfLogger(perf_path)

    def _save_reservoir_snapshot(n_found, elapsed):
        coll = _coll_ref[0]
        if coll is None or not coll.reservoir: return
        try:
            sample_data = [_serialize_grid(g) for g in coll.reservoir]
            tmp_path = sample_path.with_suffix('.tmp')
            with open(tmp_path, "w") as f: json.dump(sample_data, f, default=str)
            tmp_path.replace(sample_path)
            ckpt.save({
                "status": "running", "worker_id": worker_id,
                "partition": part_id, "sub": sub, "probe_type": probe_type,
                "n_mates_breaking": n_found, "n_found": n_found * mult,
                "n_sample": len(coll.reservoir), "elapsed_so_far": elapsed,
                "sample_file": sample_fname,
            })
        except Exception as e:
            LOG.add("task10", f"[{label}] Snapshot save error: {e}", level="error")

    _stop_requested = [False]

    def _on_mate_found(n_found, elapsed):
        rate = n_found / max(0.1, elapsed)
        now = time.time()

        if not _stop_requested[0] and stop_event and stop_event.is_set():
            _stop_requested[0] = True
            LOG.add("task10", f"[{label}] STOP requested at {n_found:,} mates", level="warning")
            _save_reservoir_snapshot(n_found, elapsed)
            perf.finalize(n_found * mult, elapsed)
            coll = _coll_ref[0]
            if coll: coll.StopSearch()
            return

        do_log = (n_found <= 10) or (n_found <= 10000 and n_found % 1000 == 0) or \
                 (n_found % 100000 == 0) or (now - last_cb[0] > 30)
        if do_log:
            LOG.add("task10", f"[{label}] {n_found:,} mates — {rate:.0f}/s ({elapsed:.0f}s)", level="math")

        if progress_callback and (now - last_cb[0] > 2 or n_found <= 10):
            pct = min(98, max(2, int(98 * n_found / n_target))) if n_target > 0 else min(95, 5 + int(elapsed / 60))
            progress_callback({
                "phase": "generate", "partition": part_id, "sub": sub,
                "probe_type": probe_type, "percent": pct,
                "message": f"{n_found:,} mates — {rate:.0f}/s ({elapsed:.0f}s)",
                "n_found": n_found * mult, "n_breaking": n_found, "rate": round(rate, 1),
            })
            last_cb[0] = now

        if n_found % 2_000_000 == 0 and n_found > _last_sample_save[0]:
            _last_sample_save[0] = n_found
            _save_reservoir_snapshot(n_found, elapsed)

        perf.maybe_log(n_found * mult, elapsed)

    sym_fn = probe["symmetry_fn"]
    use_r180 = probe["use_rot180"]

    sample, n_breaking, solver_status = _find_all_color_mates(
        DIGIT_GRID, exhaustive=True,
        max_solutions=n_target if n_target > 0 else 0,
        timeout=86400, on_solution_found=_on_mate_found,
        sample_size=50_000, use_rot180=use_r180, symmetry_fn=sym_fn,
        _collector_ref=_coll_ref, partition=part_id, partition2_values=p2_values,
    )

    elapsed = round(time.time() - t0, 1)
    n_total = n_breaking * mult
    rate = n_breaking / max(0.1, elapsed)
    status = "paused" if (stop_event and stop_event.is_set()) else "done"

    LOG.add("task10", f"[{label}] {'STOPPED' if status=='paused' else 'DONE'}: "
            f"{n_breaking:,} breaking × {mult} = {n_total:,} — {rate:.0f}/s, {elapsed:.0f}s", level="success")
    perf.finalize(n_breaking * mult, elapsed)

    sample_data = [_serialize_grid(g) for g in sample]
    try:
        tmp_path = sample_path.with_suffix('.tmp')
        with open(tmp_path, "w") as f: json.dump(sample_data, f, default=str)
        tmp_path.replace(sample_path)
    except Exception as e:
        LOG.add("task10", f"[{label}] Sample save error: {e}", level="error")

    result = {
        "status": status, "worker_id": worker_id,
        "partition": part_id, "sub": sub, "probe_type": probe_type,
        "n_mates_breaking": n_breaking, "n_found": n_total,
        "n_sample": len(sample), "elapsed": elapsed, "rate": round(rate, 1),
        "sample_file": sample_fname,
    }
    ckpt.save(result)

    if progress_callback:
        progress_callback({
            "phase": "done", "percent": 100, "probe_type": probe_type,
            "message": f"{'Stopped' if status=='paused' else 'Done'}: {n_total:,} mates ({rate:.0f}/s)",
            "n_found": n_total, "n_breaking": n_breaking, "rate": round(rate, 1),
        })

    return {"status": status, "n_found": n_total, "n_breaking": n_breaking,
            "n_target": n_target, "partition": part_id, "sub": sub,
            "probe_type": probe_type, "n_sample": len(sample), "elapsed": elapsed}


def task10_get_generation_summary(probe_type="standard"):
    total_breaking = 0
    total_sample = 0
    workers = {}
    mult = PROBE_TYPES.get(probe_type, PROBE_TYPES["standard"])["multiplier"]
    for wid in range(1, 17):
        ckpt = CheckpointManager(_ckpt_name(wid, probe_type))
        saved = ckpt.load()
        if saved:
            nb = saved.get("n_mates_breaking", 0)
            total_breaking += nb
            total_sample += saved.get("n_sample", 0)
            workers[f"G{wid}"] = {
                "n_breaking": nb, "n_total": nb * mult,
                "status": saved.get("status", "unknown"),
                "elapsed": saved.get("elapsed", 0),
            }
    return {
        "probe_type": probe_type, "multiplier": mult,
        "total_breaking": total_breaking,
        "total_found": total_breaking * mult,
        "total_sample": total_sample, "workers": workers,
    }


def task10_list_available_probes():
    result = {}
    for ptype, pinfo in PROBE_TYPES.items():
        n_workers_with_data = 0
        n_total_samples = 0
        n_done = 0
        for wid in range(1, 17):
            ckpt = CheckpointManager(_ckpt_name(wid, ptype))
            saved = ckpt.load()
            if saved:
                n_workers_with_data += 1
                n_total_samples += saved.get("n_sample", 0)
                if saved.get("status") == "done":
                    n_done += 1
                else:
                    sfname = saved.get("sample_file", _sample_filename(wid, ptype))
                    spath = Path(ckpt.filepath).parent / sfname
                    if not spath.exists():
                        n_total_samples -= saved.get("n_sample", 0)
        cat_path = _catalog_path(ptype)
        result[ptype] = {
            "label": pinfo["label"], "description": pinfo["description"],
            "multiplier": pinfo["multiplier"],
            "available": n_workers_with_data > 0,
            "n_workers_with_data": n_workers_with_data,
            "n_workers_done": n_done,
            "n_total_samples": n_total_samples,
            "catalog_exists": cat_path.exists(),
        }
    return result


# =============================================================
# Phase 2: Catalog
# =============================================================

def _canon_flat_to_dc(canon_flat):
    return ''.join(f"{v // 9}{v % 9}" for v in canon_flat)

def _do_swap_check(h, canon_flat):
    cells = []
    for v in canon_flat:
        d = v // 9; c = v % 9; cells.append((c, d))
    swapped_grid = tuple(tuple(cells[r * 9 + col] for col in range(9)) for r in range(9))
    try:
        h_swap, _, _ = fast_canonicalize_and_stab(swapped_grid)
        return h_swap == h
    except Exception: return False


def _load_all_generation_samples(probe_type="standard"):
    all_grids = []
    for wid in range(1, 17):
        ckpt = CheckpointManager(_ckpt_name(wid, probe_type))
        saved = ckpt.load()
        if not saved: continue
        status = saved.get("status", "?")
        n_mates = saved.get("n_mates_breaking", 0)
        sample_file = saved.get("sample_file")
        if sample_file:
            sample_path = Path(ckpt.filepath).parent / sample_file
            if sample_path.exists():
                try:
                    with open(sample_path) as f: sample_data = json.load(f)
                    for g in sample_data: all_grids.append(_deserialize_grid(g))
                    LOG.add("task10", f"  G{wid}/{probe_type}: loaded {len(sample_data):,} grids", level="info")
                    continue
                except Exception as e:
                    LOG.add("task10", f"  G{wid}/{probe_type}: FAILED to load {sample_file}: {e}", level="error")
        if saved.get("sample"):
            for g in saved["sample"]: all_grids.append(_deserialize_grid(g))
            continue
    LOG.add("task10", f"  TOTAL [{probe_type}]: {len(all_grids):,} grids", level="info")
    return all_grids


def _load_all_generation_samples_multi(probe_types):
    all_grids = []
    sources = []
    for pt in probe_types:
        grids = _load_all_generation_samples(pt)
        if grids:
            sources.append({"probe_type": pt, "n_grids": len(grids)})
            all_grids.extend(grids)
    return all_grids, sources


_catalog_locks = {}
_catalog_data = {}
_catalog_loaded = {}

def _get_catalog_lock(probe_type):
    if probe_type not in _catalog_locks:
        _catalog_locks[probe_type] = threading.Lock()
    return _catalog_locks[probe_type]

def _load_catalog(probe_type="standard"):
    if _catalog_loaded.get(probe_type): return
    cat_path = _catalog_path(probe_type)
    if probe_type not in _catalog_data: _catalog_data[probe_type] = {}
    if cat_path.exists():
        try:
            with open(cat_path) as f: saved = json.load(f)
            for o in saved.get("orbits", []):
                h = o["canon_hash"]
                _catalog_data[probe_type][h] = {
                    "dc_canonical": o["dc_canonical"], "stab_structural": o["stab_structural"],
                    "self_dual": o.get("self_dual", False), "sample_count": o.get("sample_count", 1),
                }
        except Exception: pass
    _catalog_loaded[probe_type] = True

def _save_catalog(probe_type="standard"):
    cat_path = _catalog_path(probe_type)
    cat_path.parent.mkdir(parents=True, exist_ok=True)
    data = _catalog_data.get(probe_type, {})
    n_orbits = len(data)
    sd_count = sum(1 for o in data.values() if o.get("self_dual"))
    stab_dist_struct = {}; stab_dist_total = {}; top_stab = 0
    for o in data.values():
        ss = o["stab_structural"]
        is_sd = o.get("self_dual", False) or False
        st = ss * 2 if is_sd else ss
        o["stab_total"] = st; o["orbit_size"] = TOTAL_STRUCTURAL // ss
        stab_dist_struct[ss] = stab_dist_struct.get(ss, 0) + 1
        stab_dist_total[st] = stab_dist_total.get(st, 0) + 1
        if st > top_stab: top_stab = st
    orbits_list = []
    for h in sorted(data, key=lambda x: -data[x].get("stab_total", 0)):
        o = data[h]
        orbits_list.append({
            "canon_hash": h, "dc_canonical": o["dc_canonical"],
            "stab_structural": o["stab_structural"],
            "stab_total": o.get("stab_total", o["stab_structural"]),
            "self_dual": o.get("self_dual", False) or False,
            "orbit_size": o.get("orbit_size", TOTAL_STRUCTURAL // o["stab_structural"]),
            "sample_count": o.get("sample_count", 1),
        })
    result = {
        "status": "done", "probe_type": probe_type, "n_orbits": n_orbits,
        "self_dual_count": sd_count, "top_stab": top_stab,
        "stab_dist_structural": {str(k): v for k, v in sorted(stab_dist_struct.items(), reverse=True)},
        "stab_dist_total": {str(k): v for k, v in sorted(stab_dist_total.items(), reverse=True)},
        "orbits": orbits_list,
    }
    with open(cat_path, "w") as f: json.dump(result, f, indent=2, default=str)


def task10_catalog_worker(worker_id, progress_callback=None, stop_event=None,
                          probe_types=None):
    if probe_types is None: probe_types = ["standard"]
    catalog_probe = probe_types[0]
    label = f"C{worker_id}/{catalog_probe}"
    LOG.add("task10", f"[{label}] START catalog (sources: {probe_types})", level="success")

    if not canon_lib_available():
        return {"status": "error", "message": "C library not available"}

    all_grids, sources_info = _load_all_generation_samples_multi(probe_types)
    if not all_grids:
        return {"status": "done", "message": f"No samples for {probe_types}", "n_new_orbits": 0, "n_orbits": 0}

    my_grids = [g for i, g in enumerate(all_grids) if i % 8 == (worker_id - 1)]
    LOG.add("task10", f"[{label}] {len(my_grids):,} grids to process", level="info")

    lock = _get_catalog_lock(catalog_probe)
    with lock:
        _load_catalog(catalog_probe)
        if catalog_probe not in _catalog_data: _catalog_data[catalog_probe] = {}

    n_new = 0; n_self_dual_found = 0; n_processed = 0; errors = 0
    t0 = time.time()

    for idx, grid in enumerate(my_grids):
        if stop_event and stop_event.is_set(): break
        n_processed += 1
        try:
            h, canon_flat, stab = fast_canonicalize_and_stab(grid)
        except Exception as e:
            errors += 1
            if errors <= 3: LOG.add("task10", f"[{label}] Canon error: {e}", level="error")
            continue

        with lock:
            cat = _catalog_data[catalog_probe]
            if h not in cat:
                cat[h] = {"dc_canonical": _canon_flat_to_dc(canon_flat), "stab_structural": stab, "self_dual": None, "sample_count": 1}
                is_new = True
            else:
                cat[h]["sample_count"] = cat[h].get("sample_count", 0) + 1; is_new = False

        if is_new:
            n_new += 1
            is_sd = _do_swap_check(h, canon_flat)
            with lock: _catalog_data[catalog_probe][h]["self_dual"] = is_sd
            if is_sd:
                n_self_dual_found += 1
                if n_self_dual_found <= 5:
                    LOG.add("task10", f"[{label}] ✦ Self-dual #{n_self_dual_found} |Stab|={stab}", level="math")

        if n_processed % 500 == 0 or n_processed == len(my_grids):
            elapsed = time.time() - t0
            rate = n_processed / max(0.1, elapsed)
            with lock:
                n_orbits = len(_catalog_data.get(catalog_probe, {}))
                sd_count = sum(1 for o in _catalog_data.get(catalog_probe, {}).values() if o.get("self_dual"))
                top_s = max((o["stab_structural"] * (2 if o.get("self_dual") else 1) for o in _catalog_data.get(catalog_probe, {}).values()), default=0)
            if progress_callback:
                progress_callback({
                    "phase": "catalog", "probe_type": catalog_probe,
                    "percent": min(98, int(98 * n_processed / len(my_grids))),
                    "message": f"{n_processed:,}/{len(my_grids):,} — {n_orbits:,} orbits (+{n_new})",
                    "n_new_orbits": n_new, "n_orbits": n_orbits, "self_dual_count": sd_count, "top_stab": top_s,
                })
            if n_processed % 2000 == 0:
                with lock: _save_catalog(catalog_probe)

    with lock:
        _save_catalog(catalog_probe)
        n_orbits = len(_catalog_data.get(catalog_probe, {}))

    elapsed = round(time.time() - t0, 1)
    LOG.add("task10", f"[{label}] DONE: +{n_new} new ({n_self_dual_found} sd), {n_orbits:,} total, {elapsed:.0f}s", level="success")

    with lock:
        cat = _catalog_data.get(catalog_probe, {})
        sd_count = sum(1 for o in cat.values() if o.get("self_dual"))
        top_s = max((o["stab_structural"] * (2 if o.get("self_dual") else 1) for o in cat.values()), default=0)
        stab_dist = {}
        for o in cat.values():
            st = o["stab_structural"] * (2 if o.get("self_dual") else 1)
            stab_dist[str(st)] = stab_dist.get(str(st), 0) + 1

    return {"status": "done", "n_new_orbits": n_new, "n_orbits": n_orbits, "probe_type": catalog_probe,
            "self_dual_count": sd_count, "top_stab": top_s, "stab_dist_total": stab_dist,
            "errors": errors, "elapsed": elapsed}


def task10_get_catalog_summary(probe_type="standard"):
    lock = _get_catalog_lock(probe_type)
    with lock:
        _load_catalog(probe_type)
        cat = _catalog_data.get(probe_type, {})
        n_orbits = len(cat)
        sd_count = sum(1 for o in cat.values() if o.get("self_dual"))
        top_s = max((o["stab_structural"] * (2 if o.get("self_dual") else 1) for o in cat.values()), default=0)
        stab_dist = {}
        for o in cat.values():
            st = o["stab_structural"] * (2 if o.get("self_dual") else 1)
            stab_dist[str(st)] = stab_dist.get(str(st), 0) + 1
    return {"probe_type": probe_type, "n_orbits": n_orbits, "self_dual_count": sd_count,
            "top_stab": top_s, "stab_dist_total": stab_dist}
