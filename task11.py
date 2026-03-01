#!/usr/bin/env python3
"""
Task 11 — Unified Orbit Catalog (multi-threaded)
Replaces: 10ab, 10eb, 10fb, 10gb, 10h, 10ib

8 worker slots (W1..W8), each with its own source dropdown.
All workers share a single catalog protected by a threading lock.
ctypes C calls release the GIL → true parallelism on canonicalization.
"""
import sys
import time
import json
import threading
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from engine import (
    CheckpointManager, fast_canonicalize_and_stab, _deserialize_grid,
    canon_lib_available, TOTAL_STRUCTURAL, LOG, EXPORT_DIR
)


def canon_flat_to_dc(canon_flat):
    """Convert canon_flat (81 ints, val = d*9 + c) to 162-char dc string."""
    return ''.join(f"{v // 9}{v % 9}" for v in canon_flat)


# ── Sources and their checkpoint patterns ──

TASK11_SOURCES = {
    "10a": {
        "label": "r180 (C2) — Back-Circulant",
        "checkpoints": [("task10a_back_circulant", "10a")],
    },
    "10e": {
        "label": "C3 involution",
        "checkpoints": [("task10e_c3_mates", "10e")],
    },
    "10f": {
        "label": "C4 transpose",
        "checkpoints": [("task10f_c4_mates", "10f")],
    },
    "10g": {
        "label": "Ordre-3 cycle (16 sub-partitions)",
        "checkpoints": [(f"task10g_p{pid}{sub}_order3", f"10g_P{pid}{sub}")
                        for pid in range(1, 9) for sub in ('a', 'b')],
    },
    "10i": {
        "label": "Sans biais — ancien (16 sub-partitions)",
        "checkpoints": [(f"task10i_p{pid}{sub}_unbiased", f"10i_P{pid}{sub}")
                        for pid in range(1, 9) for sub in ('a', 'b')],
    },
    "10": {
        "label": "Sans biais — nouveau (16 workers)",
        "checkpoints": [(f"task10_g{wid}_bc", f"10_G{wid}")
                        for wid in range(1, 17)],
    },
    "13": {
        "label": "Stab 162 — nouveau (16 workers)",
        "checkpoints": [(f"task13_g{wid}_s162", f"13_G{wid}")
                        for wid in range(1, 17)],
    },
}


# ══════════════════════════════════════════════════════════════
# Shared catalog — thread-safe singleton
# ══════════════════════════════════════════════════════════════

class SharedCatalog:
    """Thread-safe shared orbit catalog. All workers read/write through this."""

    def __init__(self):
        self.lock = threading.Lock()
        self.orbit_data = {}
        self.sources_processed = {}
        self._loaded = False
        self._dirty = False
        self._last_save = 0

    def load(self):
        with self.lock:
            if self._loaded:
                return
            ckpt = CheckpointManager("task11_catalog")
            saved = ckpt.load()
            if saved:
                self.orbit_data = saved.get("orbit_data", {})
                self.sources_processed = saved.get("sources_processed", {})
            self._loaded = True
            LOG.add("task11", f"Catalog loaded: {len(self.orbit_data)} orbits", level="info")

    def add_orbit(self, h, canon_flat, stab, source_name):
        """Add or update an orbit. Returns True if new."""
        with self.lock:
            if h not in self.orbit_data:
                self.orbit_data[h] = {
                    "dc_canonical": canon_flat_to_dc(canon_flat),
                    "stab_structural": stab,
                    "self_dual": None,
                    "sources": [source_name],
                    "sample_count": 1,
                }
                self._dirty = True
                return True
            else:
                self.orbit_data[h]["sample_count"] += 1
                if source_name not in self.orbit_data[h]["sources"]:
                    self.orbit_data[h]["sources"].append(source_name)
                    self._dirty = True
                return False

    def mark_self_dual(self, h, is_self_dual):
        with self.lock:
            if h in self.orbit_data:
                self.orbit_data[h]["self_dual"] = is_self_dual
                self._dirty = True

    def get_needs_swap(self):
        with self.lock:
            return [(h, o["dc_canonical"]) for h, o in self.orbit_data.items()
                    if o["self_dual"] is None]

    def n_orbits(self):
        with self.lock:
            return len(self.orbit_data)

    def get_summary(self):
        with self.lock:
            n = len(self.orbit_data)
            sd = sum(1 for o in self.orbit_data.values() if o.get("self_dual") is True)
            top_total = 0
            stab_dist = {}
            stab_dist_s = {}
            for o in self.orbit_data.values():
                ss = o["stab_structural"]
                is_sd = o.get("self_dual", False) or False
                st = ss * 2 if is_sd else ss
                if st > top_total:
                    top_total = st
                stab_dist[st] = stab_dist.get(st, 0) + 1
                stab_dist_s[ss] = stab_dist_s.get(ss, 0) + 1
            return {
                "n_orbits": n,
                "self_dual_count": sd,
                "top_stab": top_total,
                "sources_processed": dict(self.sources_processed),
                "stab_dist_total": {str(k): v for k, v in sorted(stab_dist.items(), reverse=True)},
                "stab_dist_structural": {str(k): v for k, v in sorted(stab_dist_s.items(), reverse=True)},
            }

    def record_source_grids(self, source_name, n_grids):
        with self.lock:
            self.sources_processed[source_name] = self.sources_processed.get(source_name, 0) + n_grids

    def save_if_dirty(self, force=False):
        now = time.time()
        if not force and (now - self._last_save < 30):
            return
        with self.lock:
            if not self._dirty and not force:
                return
            self._save_locked()
            self._last_save = now
            self._dirty = False

    def _save_locked(self):
        n_orbits = len(self.orbit_data)
        sd_count = sum(1 for o in self.orbit_data.values() if o.get("self_dual") is True)
        stab_dist_struct = {}
        stab_dist_total = {}
        top_stab = 0
        for o in self.orbit_data.values():
            ss = o["stab_structural"]
            is_sd = o.get("self_dual", False) or False
            st = ss * 2 if is_sd else ss
            o["stab_total"] = st
            o["orbit_size"] = TOTAL_STRUCTURAL // ss
            stab_dist_struct[ss] = stab_dist_struct.get(ss, 0) + 1
            stab_dist_total[st] = stab_dist_total.get(st, 0) + 1
            if st > top_stab:
                top_stab = st

        orbits_list = []
        for h in sorted(self.orbit_data, key=lambda x: -self.orbit_data[x].get("stab_total", 0)):
            o = self.orbit_data[h]
            orbits_list.append({
                "canon_hash": h,
                "dc_canonical": o["dc_canonical"],
                "stab_structural": o["stab_structural"],
                "stab_total": o.get("stab_total", o["stab_structural"]),
                "self_dual": o.get("self_dual", False) or False,
                "orbit_size": o.get("orbit_size", TOTAL_STRUCTURAL // o["stab_structural"]),
                "sources": o["sources"],
                "sample_count": o["sample_count"],
            })

        result = {
            "status": "done",
            "n_orbits": n_orbits,
            "self_dual_count": sd_count,
            "top_stab": top_stab,
            "sources_processed": dict(self.sources_processed),
            "stab_dist_structural": {str(k): v for k, v in sorted(stab_dist_struct.items(), reverse=True)},
            "stab_dist_total": {str(k): v for k, v in sorted(stab_dist_total.items(), reverse=True)},
            "orbits": orbits_list,
            "orbit_data": self.orbit_data,
        }
        ckpt = CheckpointManager("task11_catalog")
        ckpt.save(result)

        try:
            export_path = EXPORT_DIR / "task11_catalog.json"
            export_path.parent.mkdir(parents=True, exist_ok=True)
            export = {k: v for k, v in result.items() if k != "orbit_data"}
            with open(export_path, "w") as f:
                json.dump(export, f, indent=2, default=str)
        except Exception:
            pass


# Global singleton
_catalog = SharedCatalog()


def _load_grids_from_checkpoint(ckpt_name):
    ckpt = CheckpointManager(ckpt_name)
    saved = ckpt.load()
    if not saved:
        return []
    # Accept both "done" and partial results (status="running") that have samples
    if saved.get("status") not in ("done", "running"):
        return []
    raw = ckpt.load_sample()
    if raw:
        return [_deserialize_grid(g) for g in raw]
    return []


# ══════════════════════════════════════════════════════════════
# Public API
# ══════════════════════════════════════════════════════════════

def task11_get_available_sources():
    result = {}
    for src_name, src_info in TASK11_SOURCES.items():
        n_sub = 0
        for ckpt_name, _ in src_info["checkpoints"]:
            saved = CheckpointManager(ckpt_name).load()
            if saved and saved.get("status") == "done":
                n_sub += 1
        result[src_name] = {
            "label": src_info["label"],
            "available": n_sub > 0,
            "n_sub_done": n_sub,
            "n_sub_total": len(src_info["checkpoints"]),
        }
    return result


def task11_get_catalog_summary():
    _catalog.load()
    return _catalog.get_summary()


def _do_swap_check(h, canon_flat):
    """Swap d↔c on canonical form, re-canonicalize, check if same orbit."""
    # Build swapped grid directly from canon_flat (no string round-trip)
    cells = []
    for v in canon_flat:
        d = v // 9
        c = v % 9
        cells.append((c, d))   # swap: new_digit=old_color, new_color=old_digit
    swapped_grid = tuple(
        tuple(cells[r * 9 + col] for col in range(9))
        for r in range(9)
    )
    try:
        h_swap, _, _ = fast_canonicalize_and_stab(swapped_grid)
        return h_swap == h
    except:
        return False


def task11_worker(worker_id, source_name, progress_callback=None, stop_event=None):
    """
    One worker thread: loads grids from source, canonicalizes, adds to shared catalog.
    Swap check is done INLINE for each new orbit (no separate phase).
    worker_id: 1..8, source_name: '10a'|'10e'|'10f'|'10g'|'10i'|'10'|'13'
    """
    label = f"W{worker_id}"

    if not canon_lib_available():
        msg = "canon.so introuvable"
        LOG.add("task11", f"[{label}] {msg}", level="error")
        return {"status": "error", "message": msg}

    if source_name not in TASK11_SOURCES:
        msg = f"Source inconnue: {source_name}"
        LOG.add("task11", f"[{label}] {msg}", level="error")
        return {"status": "error", "message": msg}

    _catalog.load()
    source_info = TASK11_SOURCES[source_name]
    all_checkpoints = source_info["checkpoints"]

    # ── Auto-split: worker N takes checkpoints where index % 8 == (N-1) ──
    # 1 checkpoint (10a/10e/10f): only W1 gets work
    # 16 checkpoints (10g/10i): each worker gets 2 sub-partitions
    my_checkpoints = [(idx, cp) for idx, cp in enumerate(all_checkpoints)
                      if idx % 8 == (worker_id - 1)]

    if not my_checkpoints:
        LOG.add("task11", f"[{label}] Source {source_name}: aucun checkpoint pour ce worker "
                f"(total={len(all_checkpoints)}, seul W1 travaille)", level="warning")
        return {"status": "done", "worker": worker_id, "source": source_name,
                "n_grids_processed": 0, "n_new_orbits": 0, "n_orbits": _catalog.n_orbits(),
                "message": f"No checkpoints for W{worker_id} (source has {len(all_checkpoints)})",
                **_catalog.get_summary(), "errors": 0, "elapsed": 0}

    n_orbits_before = _catalog.n_orbits()
    my_labels = [cp[1] for _, cp in my_checkpoints]
    LOG.add("task11", f"[{label}] START — {source_name}, checkpoints: {my_labels}, "
            f"catalog={n_orbits_before} orbits", level="success")

    t0 = time.time()
    n_grids_done = 0
    n_new = 0
    n_self_dual_found = 0
    errors = 0
    stopped = False
    total_grids_est = len(my_checkpoints) * 100_000  # estimate for progress

    for cp_idx, (_, (ckpt_name, sub_label)) in enumerate(my_checkpoints):
        if stop_event and stop_event.is_set():
            stopped = True
            break

        grids = _load_grids_from_checkpoint(ckpt_name)
        if not grids:
            total_grids_est -= 100_000  # adjust estimate
            continue

        n_batch = len(grids)
        total_grids_est = total_grids_est - 100_000 + n_batch  # replace estimate with actual
        LOG.add("task11", f"[{label}] {sub_label}: {n_batch:,} grids "
                f"({cp_idx+1}/{len(my_checkpoints)})", level="info")

        for i, grid in enumerate(grids):
            if stop_event and stop_event.is_set():
                stopped = True
                break

            try:
                h, canon_flat, stab = fast_canonicalize_and_stab(grid)
            except Exception as e:
                errors += 1
                if errors <= 3:
                    LOG.add("task11", f"[{label}] Canon error: {e}", level="error")
                continue

            is_new = _catalog.add_orbit(h, canon_flat, stab, source_name)
            if is_new:
                n_new += 1
                # ── Inline swap check ──
                is_sd = _do_swap_check(h, canon_flat)
                _catalog.mark_self_dual(h, is_sd)
                if is_sd:
                    n_self_dual_found += 1
                    if n_self_dual_found <= 5:
                        LOG.add("task11",
                            f"[{label}] ✦ Self-dual orbit #{n_self_dual_found} |Stab|={stab}",
                            level="math")

            n_grids_done += 1

            if (i + 1) % 500 == 0 or (i + 1) == n_batch:
                elapsed = time.time() - t0
                rate = n_grids_done / max(0.1, elapsed)
                total_orbits = _catalog.n_orbits()
                summary = _catalog.get_summary()
                pct = min(98, max(3, int(98 * n_grids_done / max(1, total_grids_est))))
                LOG.add("task11",
                    f"[{label}] {sub_label} {i+1:,}/{n_batch:,} — "
                    f"{total_orbits:,} orbits (+{n_new} new, {summary['self_dual_count']} sd) "
                    f"top|Stab|={summary['top_stab']} {rate:.0f}/s",
                    level="math")
                if progress_callback:
                    progress_callback({
                        "phase": "canon",
                        "percent": pct,
                        "message": f"{sub_label}: {i+1:,}/{n_batch:,} — {total_orbits:,} orb (+{n_new}, {n_self_dual_found} sd) {rate:.0f}/s",
                        "n_orbits": total_orbits,
                        "n_new": n_new,
                        "n_grids": n_grids_done,
                        "n_self_dual": summary["self_dual_count"],
                        "source": source_name,
                    })
                _catalog.save_if_dirty()

    # ── Finalize ──
    _catalog.record_source_grids(source_name, n_grids_done)
    _catalog.save_if_dirty(force=True)

    elapsed = round(time.time() - t0, 1)
    total_orbits = _catalog.n_orbits()
    summary = _catalog.get_summary()

    LOG.add("task11", f"[{label}] {'STOPPED' if stopped else 'DONE'}: "
            f"+{n_new} new ({n_self_dual_found} self-dual), "
            f"{total_orbits} total, top|Stab|={summary['top_stab']}, {elapsed:.0f}s",
            level="success")

    if progress_callback:
        progress_callback({"phase": "done", "percent": 100,
                           "message": f"{'Stopped' if stopped else 'Done'}: +{n_new} new ({n_self_dual_found} sd), {total_orbits} total"})

    return {
        "status": "paused" if stopped else "done",
        "worker": worker_id, "source": source_name,
        "n_grids_processed": n_grids_done, "n_new_orbits": n_new,
        "n_orbits": total_orbits,
        "top_stab": summary["top_stab"],
        "self_dual_count": summary["self_dual_count"],
        "sources_processed": summary["sources_processed"],
        "stab_dist_total": summary["stab_dist_total"],
        "stab_dist_structural": summary.get("stab_dist_structural", {}),
        "errors": errors, "elapsed": elapsed,
    }
