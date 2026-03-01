/*
 * canon.c - Fast canonicalization and stabilizer for Suirodoku 9x9
 *
 * Compile:
 *   Windows: gcc -O3 -shared -o canon.dll canon.c
 *   Linux:   gcc -O3 -shared -fPIC -o canon.so canon.c
 *
 * Single pass over 3,359,232 transforms:
 *   - Finds canonical form (lex minimum over all transforms + relabeling)
 *   - Counts stabilizer size (how many transforms fix the grid up to relabeling)
 *   - ~0.3s per grid with -O3
 */

#include <string.h>

#define N 9
#define NCELLS 81

static const int S3[6][3] = {
    {0,1,2}, {0,2,1}, {1,0,2}, {1,2,0}, {2,0,1}, {2,1,0}
};

#ifdef _WIN32
#define EXPORT __declspec(dllexport)
#else
#define EXPORT __attribute__((visibility("default")))
#endif

/*
 * canonicalize_and_stab
 *
 * Inputs:
 *   grid_d[81] - digits 0-8, row-major
 *   grid_k[81] - colors 0-8, row-major
 *
 * Outputs:
 *   canon_out[81] - canonical form (val = relabeled_d * 9 + relabeled_c)
 *   stab_out      - pointer to int, receives stabilizer size
 *
 * Returns 0 on success.
 */
EXPORT int canonicalize_and_stab(
    const int *grid_d,
    const int *grid_k,
    int *canon_out,
    int *stab_out
) {
    int best[NCELLS];
    int stab = 0;
    int first = 1;
    int rp[9], cp[9];

    for (int ibp = 0; ibp < 6; ibp++) {
        const int *bp = S3[ibp];

        for (int irb0 = 0; irb0 < 6; irb0++) {
        for (int irb1 = 0; irb1 < 6; irb1++) {
        for (int irb2 = 0; irb2 < 6; irb2++) {
            /* Build row permutation */
            const int *rbs[3];
            rbs[0] = S3[irb0]; rbs[1] = S3[irb1]; rbs[2] = S3[irb2];
            for (int bi = 0; bi < 3; bi++) {
                int sb = bp[bi];
                for (int ri = 0; ri < 3; ri++)
                    rp[bi*3+ri] = sb*3 + rbs[sb][ri];
            }

            for (int isp = 0; isp < 6; isp++) {
                const int *sp = S3[isp];

                for (int ics0 = 0; ics0 < 6; ics0++) {
                for (int ics1 = 0; ics1 < 6; ics1++) {
                for (int ics2 = 0; ics2 < 6; ics2++) {
                    /* Build col permutation */
                    const int *css[3];
                    css[0] = S3[ics0]; css[1] = S3[ics1]; css[2] = S3[ics2];
                    for (int si = 0; si < 3; si++) {
                        int ss = sp[si];
                        for (int ci = 0; ci < 3; ci++)
                            cp[si*3+ci] = ss*3 + css[ss][ci];
                    }

                    for (int rot = 0; rot < 2; rot++) {

                        /* ── Precompute source positions ── */
                        int src[NCELLS]; /* src[i] = flat index into original grid */
                        for (int r = 0; r < N; r++) {
                            for (int c = 0; c < N; c++) {
                                int sr, sc;
                                if (rot == 0) { sr = rp[r]; sc = cp[c]; }
                                else           { sr = rp[8-c]; sc = cp[r]; }
                                src[r*N+c] = sr*N+sc;
                            }
                        }

                        /* ── Stabilizer check (early bailout) ── */
                        {
                            int sdm[9], scm[9];
                            memset(sdm, -1, sizeof(sdm));
                            memset(scm, -1, sizeof(scm));
                            int ok = 1;
                            for (int i = 0; i < NCELLS; i++) {
                                int od = grid_d[i], ok_ = grid_k[i];
                                int td = grid_d[src[i]], tk = grid_k[src[i]];
                                if (sdm[od] < 0) sdm[od] = td;
                                else if (sdm[od] != td) { ok = 0; break; }
                                if (scm[ok_] < 0) scm[ok_] = tk;
                                else if (scm[ok_] != tk) { ok = 0; break; }
                            }
                            if (ok) {
                                /* Verify bijection */
                                int du[9] = {0}, ku[9] = {0};
                                for (int i = 0; i < 9; i++) {
                                    if (sdm[i] >= 0 && sdm[i] < 9) du[sdm[i]] = 1;
                                    if (scm[i] >= 0 && scm[i] < 9) ku[scm[i]] = 1;
                                }
                                int bij = 1;
                                for (int i = 0; i < 9; i++)
                                    if (!du[i] || !ku[i]) { bij = 0; break; }
                                if (bij) stab++;
                            }
                        }

                        /* ── Canonicalization: relabel + lex compare ── */
                        {
                            /* Digit relabeling from transformed row 0 */
                            int dmap[9];
                            for (int c2 = 0; c2 < N; c2++)
                                dmap[grid_d[src[c2]]] = c2;

                            /* Color relabeling: first-appearance order */
                            /* Lex compare with early exit */
                            int cmap[9];
                            memset(cmap, -1, sizeof(cmap));
                            int cnext = 0;
                            int cmp = 0; /* 0=equal so far, -1=new best, 1=worse */

                            for (int i = 0; i < NCELLS; i++) {
                                int td = grid_d[src[i]];
                                int tk = grid_k[src[i]];
                                if (cmap[tk] < 0) cmap[tk] = cnext++;
                                int val = dmap[td] * N + cmap[tk];

                                if (first) {
                                    best[i] = val;
                                } else if (cmp == 0) {
                                    if (val < best[i]) {
                                        cmp = -1;
                                        best[i] = val;
                                    } else if (val > best[i]) {
                                        cmp = 1;
                                        break; /* worse, skip rest */
                                    }
                                    /* equal: continue */
                                } else {
                                    /* cmp == -1: filling in new best */
                                    best[i] = val;
                                }
                            }
                            first = 0;
                        }

                    } /* rot */
                }}} /* ics */
            } /* isp */
        }}} /* irb */
    } /* ibp */

    memcpy(canon_out, best, NCELLS * sizeof(int));
    *stab_out = stab;
    return 0;
}


/*
 * canonicalize_only - just canonical form, no stabilizer (faster)
 */
EXPORT int canonicalize_only(
    const int *grid_d,
    const int *grid_k,
    int *canon_out
) {
    int best[NCELLS];
    int first = 1;
    int rp[9], cp[9];

    for (int ibp = 0; ibp < 6; ibp++) {
        const int *bp = S3[ibp];
        for (int irb0 = 0; irb0 < 6; irb0++) {
        for (int irb1 = 0; irb1 < 6; irb1++) {
        for (int irb2 = 0; irb2 < 6; irb2++) {
            const int *rbs[3];
            rbs[0] = S3[irb0]; rbs[1] = S3[irb1]; rbs[2] = S3[irb2];
            for (int bi = 0; bi < 3; bi++) {
                int sb = bp[bi];
                for (int ri = 0; ri < 3; ri++)
                    rp[bi*3+ri] = sb*3 + rbs[sb][ri];
            }
            for (int isp = 0; isp < 6; isp++) {
                const int *sp = S3[isp];
                for (int ics0 = 0; ics0 < 6; ics0++) {
                for (int ics1 = 0; ics1 < 6; ics1++) {
                for (int ics2 = 0; ics2 < 6; ics2++) {
                    const int *css[3];
                    css[0] = S3[ics0]; css[1] = S3[ics1]; css[2] = S3[ics2];
                    for (int si = 0; si < 3; si++) {
                        int ss = sp[si];
                        for (int ci = 0; ci < 3; ci++)
                            cp[si*3+ci] = ss*3 + css[ss][ci];
                    }
                    for (int rot = 0; rot < 2; rot++) {
                        int src[NCELLS];
                        for (int r = 0; r < N; r++)
                            for (int c = 0; c < N; c++) {
                                int sr, sc;
                                if (rot == 0) { sr = rp[r]; sc = cp[c]; }
                                else           { sr = rp[8-c]; sc = cp[r]; }
                                src[r*N+c] = sr*N+sc;
                            }

                        int dmap[9];
                        for (int c2 = 0; c2 < N; c2++)
                            dmap[grid_d[src[c2]]] = c2;

                        int cmap[9];
                        memset(cmap, -1, sizeof(cmap));
                        int cnext = 0;
                        int cmp = 0;

                        for (int i = 0; i < NCELLS; i++) {
                            int td = grid_d[src[i]];
                            int tk = grid_k[src[i]];
                            if (cmap[tk] < 0) cmap[tk] = cnext++;
                            int val = dmap[td]*N + cmap[tk];

                            if (first) {
                                best[i] = val;
                            } else if (cmp == 0) {
                                if (val < best[i]) { cmp = -1; best[i] = val; }
                                else if (val > best[i]) { cmp = 1; break; }
                            } else {
                                best[i] = val;
                            }
                        }
                        first = 0;
                    }
                }}}
            }
        }}}
    }

    memcpy(canon_out, best, NCELLS * sizeof(int));
    return 0;
}
