SUIRODOKU LAB v12
==================

Computation engine for exhaustive enumeration, orbit classification,
and minimum-clue analysis of 9x9 Suirodoku grids.

Author:      Jordan Maire -- suirodoku.com
University:  Paris 1 Pantheon-Sorbonne
Paper:       "There Is a 16-Clue Suirodoku" (February 2026)


WHAT IS A SUIRODOKU?
--------------------

A Suirodoku is a 9x9 grid where each cell holds a pair (digit, color)
such that:

  1. The digits form a valid Sudoku (AllDifferent per row, column, 3x3 box)
  2. The colors also form a valid Sudoku
  3. All 81 pairs (digit, color) are distinct

It is a Graeco-Latin square subject to Sudoku block constraints.
The Diaz encoding (k = d*9 + c) reduces the global constraint to a
single AllDifferent over 81 variables. The CP-SAT model uses 55
AllDifferent constraints in total (54 local + 1 global).


KEY RESULTS
-----------

  Exact 4x4 Suirodoku grids ............ 2,304
  Minimum clues 4x4 (proven) ........... 4
  Back-circulant mates (exact) .......... 3,622,317,453
  BC orbits (catalog) ................... 740
  Stab-162 orbits (catalog) ............. 1,129
  Minimum clues 9x9 .................... 14 (vs 17 for classical Sudoku)
  Crystal Grid minimum clues ............ 27
  Crystal Grid total stabilizer ......... 1,296 (absolute maximum, unique)
  Suirodoku symmetry group .............. ~8.85e17 (~726,000x Sudoku)
  Structural group ...................... 3,359,232 elements


ARCHITECTURE
------------

  suirodoku_lab/
    engine.py           Core engine: Tasks 01-07, utilities, LabState, checkpointing
    app.py              Flask server (port 5000) + APIs + thread management
    task10.py           Task 10: Back-circulant generation + orbit catalog
    task11.py           Task 11: Unified orbit catalog (multiple sources)
    task13.py           Task 13: Stab-162 generation + independent catalog
    worker_task12.py    Task 12: Crystal Grid minimum clues (external worker)
    worker_10i.py       External worker for Task 10i (unbiased enumeration)
    canon.c             C canonicalization module source
    static/
      dashboard.html    Web UI (tabs: Compute / Grids / Collab / Log)
    checkpoints/        Per-task JSON saves (crash recovery)
    exports/            Exportable JSON files
    imports/            Import files for multi-worker merge


GETTING STARTED
---------------

  pip install flask ortools
  cd D:\suirodoku_lab
  py app.py

  Then open http://localhost:5000


TASKS
-----

Dashboard tasks:

  01  Exact 4x4 Count        Enumerates all 2,304 Suirodoku 4x4 grids via exhaustive CP-SAT
  02  Orbit Analysis          Computes stabilizer of the model grid (stab=2, orbit ~4.42e17)
  03  Grid Search 9x9         Finds new grids via CP-SAT with random post-solve rotation (3,286 found)
  04  Orbit Classification    Canonicalizes grids into distinct orbits (2,583 orbits, C module)
  05  Min Clue 4x4            Minimum clues for 4x4: result is 4 (proven)
  06  Color Mate Count        How many color layers exist for a given digit Sudoku?
  07  Min Clue 9x9            Minimum clues for 9x9 via greedy removal + CP-SAT uniqueness check
  10  Back-Circulant          Exhaustive mate generation (16 partitioned workers) + orbit catalog (8 workers)
  11  Orbit Catalog           Unified catalog: canonicalization + swap check (self-dual) from multiple sources
  12  Crystal Grid Min Clues  Minimum clues for the Crystal Grid (stab=1296). Result: 27 clues
  13  Stab-162 Grid           Generation + independent catalog for the stab=162 digit grid

External workers (command line, independent from dashboard):

  python worker_task12.py 0 8 200          Task 12: Crystal Grid min clues
  python worker_task12.py 1 8 200          (8 workers, 200 shuffles each)
  python worker_10i.py <partition> <sub>   Task 10i: unbiased BC enumeration


KEY METHODS
-----------

Canonicalization (C module)

  Canonicalizing a Suirodoku grid requires testing all 3,359,232 structural
  transforms combined with relabelings (S9 x S9). The C module (canon_lib)
  does this in ~0.15s/grid vs ~6 min in pure Python, a x2,400 speedup.
  This is the critical component that makes orbit analysis feasible.

Greedy Removal (minimum clues)

  To find the minimum number of clues, each grid undergoes N passes of
  greedy removal in random order. At each step, a clue is tentatively
  removed and CP-SAT checks whether the solution remains unique. The best
  pass gives an upper bound, then a criticality check proves every
  remaining clue is necessary.

CP-SAT Partitioning

  For exhaustive mate enumeration (Task 10), the search space is partitioned
  by fixing color[0][1] and color[0][2]. This yields 8 partitions x 2
  sub-partitions = 16 independent workers, each running EnumerateAllSolutions.


NOTABLE GRIDS
-------------

The Back-Circulant (BC)

  The most symmetric Sudoku grid (|Aut| = 648, unique). Cayley table of
  Z3 x Z3. Admits 3,622,317,453 Suirodoku mates, organized into 740
  orbits. Subramani (2012) had found only 9.

The Crystal Grid

  The most symmetric Suirodoku (|Aut_total| = 1,296, unique). Self-dual
  (invariant under digit/color swap). Minimum clues = 27, the highest
  observed across all tested orbits. It is the Suirodoku analogue of the
  back-circulant in classical Sudoku.

  dc_canonical:
    001122334455667788364758607182031425637485061728304152
    122001455334788667485637728061152304758364182607425031
    210210543543876876573846816270240513846573270816513240

The Stab-162

  The Sudoku grid with |Aut| = 162 (unique). Admits ~5.5 billion breaking
  mates, organized into 1,129 orbits. Independent catalog via Task 13.


BUG FIX HISTORY (v10 to v12)
-----------------------------

x2 Bug in Structural Group (v10 to v11)

  enumerate_structural_transforms() iterated over 4 rotations (0/90/180/270)
  x 1296^2 permutations. However, the 180-degree rotation is already
  contained in the row/column permutations. The true group has 3,359,232
  elements, not 6,718,464. All stabilizers were artificially doubled.
  Fixed in v11: range(2) instead of range(4).

r180 Bias in Solver (v10 to v11)

  Task 3 imposed r180/r90 symmetry, making grids without these symmetries
  invisible. Fixed in v11 with random post-solve rotation.

Task 10 Consolidation (v11 to v12)

  The former 10a-10i (9 sub-tasks) were merged into a single task10.py
  with generation (16 workers) + catalog (8 workers), all pauseable from
  the dashboard.


DEPENDENCIES
------------

  flask
  ortools

  Optional C module: canon_lib (fast canonicalization). Without it, the
  system falls back to pure Python (x2,400 slower for canonicalization).


TESTED ENVIRONMENT
------------------

  - Windows 10/11, Python 3.14
  - AMD Ryzen 7 8700F (16 threads)
  - Secondary laptop: 12 cores / 16 logical
  - Both machines communicate via JSON export/import for multi-worker merge
