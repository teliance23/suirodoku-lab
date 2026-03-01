# Suirodoku Lab v10 — Unified

**3 fichiers, c'est tout :**
- `engine.py` — Moteur de calcul (Tasks 01-10d)
- `app.py` — Serveur web Flask (port 5000)
- `static/dashboard.html` — Interface web

## Lancement

```
cd D:\suirodoku_lab
py app.py
```
→ Ouvrir http://localhost:5000

## Les Tasks

| Task | Nom | Ce qu'elle fait |
|------|-----|-----------------|
| **01** | Exact 4×4 Count | Compte le nombre exact de grilles Suirodoku 4×4 (résultat: 2304) |
| **02** | Orbit Analysis | Calcule la taille de l'orbite de la grille modèle du site (résultat: 4.42×10¹⁷) |
| **03** | Grid Search 9×9 | Cherche de nouvelles grilles 9×9 par construction CP-SAT (60s par tentative) |
| **04** | Orbit Classification | Classe les grilles de Task 03 en orbites distinctes (3.35M transforms par grille) |
| **05** | Min Clue 4×4 | Cherche le puzzle minimal 4×4 (plus petit nombre d'indices avec solution unique) |
| **06** | Mate Count | Pour chaque Sudoku, compte combien de couches couleur sont compatibles |
| **07** | Min Clue 9×9 | Cherche le puzzle minimal 9×9 (retrait d'indices + vérification unicité) |
| **08** | Detailed Stabilizers | Analyse détaillée des stabilisateurs de chaque orbite |
| **09** | σ↔τ Swap | Teste si échanger chiffres↔couleurs fusionne des orbites |
| **10a** | Back-Circulant Mates | Trouve TOUS les mates couleur du Sudoku back-circulant (exhaustif) |
| **10b** | Cyclic Mates | Même chose pour d'autres Sudoku cycliques |
| **10c** | Imposed Symmetry | Cherche des grilles avec symétrie forcée (rot90, band_cyclic...) |
| **10d** | Swap + Transform | Cherche des grilles auto-duales (digit↔color exchange) |

## Pipeline

```
Task 03 (cherche des grilles) ─────┐
Task 10a/b/c/d (hunter) ──────────┤
                                   ▼
                        Task 04 (classifie en orbites)
                                   │
                    ┌──────────────┼──────────────┐
                    ▼              ▼              ▼
              Task 07         Task 08         Task 09
           (min clues)     (stabilisateurs)  (swap σ↔τ)
```

**Task 10 injecte automatiquement dans Task 03.** Après avoir lancé n'importe quelle Task 10x, relance Task 04 pour classifier les nouvelles grilles.

## Fichiers supprimés (intégrés dans engine.py)

- ~~config.py~~
- ~~migrate.py~~
- ~~import_hunter.py~~
- ~~symmetry_hunter.py~~
