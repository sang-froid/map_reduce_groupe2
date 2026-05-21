"""
benchmark.py
Compare K-means classique vs MapReduce chunk vs MapReduce multi-serveurs.
Compatible Python 3.12+, Windows/Linux/Mac.
"""
import time
import json
import os
import sys
import threading

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from kmeans_classic import load_data, kmeans_classic
from kmeans_mapreduce import run_kmeans_mapreduce, run_kmeans_mapreduce_distributed

RESULTS_DIR = "results"
os.makedirs(RESULTS_DIR, exist_ok=True)

K         = 3
MAX_ITER  = 10
TIMEOUT_S = 20

SERVER_FILES = [
    "data/server_nord.csv",
    "data/server_centre.csv",
    "data/server_sud.csv",
    "data/server_ouest.csv",
    "data/server_est.csv",
]

# ─────────────────────────────────────────────
# Timeout cross-platform (threading)
# ─────────────────────────────────────────────
class TimedOut(Exception):
    pass

def run_with_timeout(func, args, timeout):
    result = [None]
    error  = [None]

    def target():
        try:
            result[0] = func(*args)
        except Exception as e:
            error[0] = e

    t = threading.Thread(target=target, daemon=True)
    t.start()
    t.join(timeout)
    if t.is_alive():
        raise TimedOut(f"Timeout après {timeout}s")
    if error[0]:
        raise error[0]
    return result[0]


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────
def fmt_time(row, key_time, key_timeout):
    if row.get(key_timeout):
        return str(row[key_time])
    return f"{row[key_time]}s"


# ─────────────────────────────────────────────
# Benchmark principal
# ─────────────────────────────────────────────
DATASETS = [
    ("data/all_sensors.csv",   "1 500 capteurs (normal)"),
    ("data/large_sensors.csv", "50 000 capteurs (stress)"),
]

def run_benchmark():
    results = []
    print("\n" + "="*70)
    print("  BENCHMARK : CLASSIQUE  vs  MAPREDUCE CHUNK  vs  MULTI-SERVEURS")
    print("="*70)

    # ── Scénarios 1 & 2 : classique vs MapReduce chunk ────────────────────
    for filepath, label in DATASETS:
        if not os.path.exists(filepath):
            print(f"[SKIP] {filepath} manquant")
            continue

        print(f"\n{'─'*70}")
        print(f"  Dataset : {label}")
        print(f"{'─'*70}")

        points = load_data(filepath)
        row = {"dataset": label, "n_points": len(points)}

        # Classique
        print(f"\n[1/2] K-MEANS CLASSIQUE")
        timeout = TIMEOUT_S if len(points) > 5000 else 0
        if timeout:
            print(f"  ⚠ Timeout activé : {timeout}s")
        try:
            if timeout:
                _, _, iters, dur = run_with_timeout(
                    kmeans_classic, (points, K, MAX_ITER), timeout
                )
            else:
                _, _, iters, dur = kmeans_classic(points, k=K, max_iter=MAX_ITER)
            row.update({"classic_time": round(dur, 3), "classic_iters": iters,
                        "classic_timeout": False})
        except TimedOut:
            row.update({"classic_time": f">{timeout}s", "classic_iters": "—",
                        "classic_timeout": True})
            print(f"\n  ⏱ Timeout atteint !")

        # MapReduce chunk
        print(f"\n[2/2] K-MEANS MAPREDUCE (chunk)")
        try:
            _, _, iters_mr, dur_mr = run_kmeans_mapreduce(
                filepath, k=K, max_iter=MAX_ITER
            )
            row.update({"mr_time": round(dur_mr, 3), "mr_iters": iters_mr,
                        "mr_timeout": False})
        except Exception as e:
            row.update({"mr_time": "ERREUR", "mr_iters": "—", "mr_timeout": True})
            print(f"  Erreur : {e}")

        results.append(row)

    # ── Scénario 3 : MapReduce multi-serveurs ──────────────────────────────
    missing = [f for f in SERVER_FILES if not os.path.exists(f)]
    if not missing:
        print(f"\n{'─'*70}")
        print(f"  Scénario distribué : {len(SERVER_FILES)} serveurs régionaux")
        print(f"{'─'*70}")

        row_srv = {"dataset": "multi-serveurs (1 500)", "n_points": 0,
                   "classic_timeout": False, "mr_timeout": False}

        # Classique sur all_sensors.csv (équivalent agrégé)
        print(f"\n[1/2] K-MEANS CLASSIQUE (données agrégées)")
        points = load_data("data/all_sensors.csv")
        _, _, iters, dur = kmeans_classic(points, k=K, max_iter=MAX_ITER)
        row_srv.update({"classic_time": round(dur, 3), "classic_iters": iters,
                        "n_points": len(points)})

        # MapReduce distribué : un worker par serveur
        print(f"\n[2/2] K-MEANS MAPREDUCE (multi-serveurs)")
        _, cluster_counts, iters_mr, dur_mr = run_kmeans_mapreduce_distributed(
            SERVER_FILES, k=K, max_iter=MAX_ITER
        )
        row_srv.update({"mr_time": round(dur_mr, 3), "mr_iters": iters_mr,
                        "n_points": sum(cluster_counts.values())})

        results.append(row_srv)
    else:
        print(f"\n[SKIP] Fichiers serveurs manquants : {missing}")
        print("       Lancez d'abord : python generate_data.py")

    # ── Tableau récapitulatif ──────────────────────────────────────────────
    print("\n\n" + "="*70)
    print("  RÉSULTATS COMPARATIFS")
    print("="*70)
    print(f"{'Dataset':<30} {'Points':>8} {'Classique':>16} {'MapReduce':>14}")
    print("─"*70)
    for r in results:
        c = fmt_time(r, "classic_time", "classic_timeout")
        m = fmt_time(r, "mr_time", "mr_timeout")
        print(f"{r['dataset']:<30} {r['n_points']:>8} {c:>16} {m:>14}")
    print("="*70)

    for r in results:
        classic_ok = not r.get("classic_timeout") and r.get("classic_time") not in (None, "ERREUR")
        mr_ok      = not r.get("mr_timeout")      and r.get("mr_time")      not in (None, "ERREUR")
        if classic_ok and mr_ok:
            try:
                ratio = round(float(str(r["mr_time"])) / float(str(r["classic_time"])), 2)
                if ratio < 1.0:
                    print(f"  → MapReduce {1/ratio:.2f}x PLUS RAPIDE  ({r['dataset']})")
                else:
                    print(f"  → Overhead MapReduce : {ratio}x  ({r['dataset']}) "
                          f"— overhead réseau simulé en local, s'inverse à grande échelle")
            except Exception:
                pass
        elif r.get("classic_timeout") and mr_ok:
            print(f"  → Classique TIMEOUT ({r['dataset']}) : MapReduce termine normalement ✓")

    out = os.path.join(RESULTS_DIR, "benchmark.json")
    with open(out, "w") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    print(f"\n  Résultats sauvegardés → {out}\n")
    return results


if __name__ == "__main__":
    import multiprocessing
    multiprocessing.freeze_support()  # nécessaire sur Windows
    run_benchmark()
