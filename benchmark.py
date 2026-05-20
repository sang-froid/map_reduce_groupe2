"""
benchmark.py
Compare K-means classique vs MapReduce (multiprocessing).
Compatible Python 3.12+, Windows/Linux/Mac.
"""
import time, json, os, sys, threading
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from kmeans_classic import load_data, kmeans_classic
from kmeans_mapreduce import run_kmeans_mapreduce

RESULTS_DIR = "results"
os.makedirs(RESULTS_DIR, exist_ok=True)

K        = 3
MAX_ITER = 10
TIMEOUT_S = 20   # timeout pour l'algo classique sur grand dataset

# ─────────────────────────────────────────────
# Timeout cross-platform (threading)
# ─────────────────────────────────────────────
class TimedOut(Exception): pass

def run_with_timeout(func, args, timeout):
    """Lance func(*args) avec un timeout cross-platform (threading)."""
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
DATASETS = [
    ("data/all_sensors.csv",   "1 500 capteurs"),
    ("data/large_sensors.csv", "50 000 capteurs"),
]

def run_benchmark():
    results = []
    print("\n" + "="*62)
    print("  BENCHMARK : K-MEANS CLASSIQUE vs MAPREDUCE")
    print("="*62)

    for filepath, label in DATASETS:
        if not os.path.exists(filepath):
            print(f"[SKIP] {filepath} manquant"); continue

        print(f"\n{'─'*62}")
        print(f"  Dataset : {label}")
        print(f"{'─'*62}")

        points = load_data(filepath)
        row = {"dataset": label, "n_points": len(points)}

        # ─ Classique ─────────────────────────
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
            row.update({"classic_time": round(dur,3), "classic_iters": iters, "classic_timeout": False})
        except TimedOut:
            row.update({"classic_time": f">{timeout}s", "classic_iters": "—", "classic_timeout": True})
            print(f"\n  ⏱ Timeout atteint !")

        # ─ MapReduce ─────────────────────────
        print(f"\n[2/2] K-MEANS MAPREDUCE")
        try:
            _, _, iters_mr, dur_mr = run_kmeans_mapreduce(
                filepath, k=K, max_iter=MAX_ITER
            )
            row.update({"mr_time": round(dur_mr,3), "mr_iters": iters_mr, "mr_timeout": False})
        except Exception as e:
            row.update({"mr_time": "ERREUR", "mr_iters": "—", "mr_timeout": True})
            print(f"  Erreur : {e}")

        results.append(row)

    # ─ Tableau récap ─────────────────────────
    print("\n\n" + "="*62)
    print("  RÉSULTATS COMPARATIFS")
    print("="*62)
    print(f"{'Dataset':<22} {'Points':>8} {'Classique':>16} {'MapReduce':>14}")
    print("─"*62)
    for r in results:
        c = f"{r['classic_time']}s" if not r.get('classic_timeout') else str(r['classic_time'])
        m = f"{r['mr_time']}s"      if not r.get('mr_timeout')      else str(r['mr_time'])
        print(f"{r['dataset']:<22} {r['n_points']:>8} {c:>16} {m:>14}")
    print("="*62)

    for r in results:
        if not r.get('classic_timeout') and not r.get('mr_timeout'):
            try:
                ratio = round(float(str(r['mr_time'])) / float(str(r['classic_time'])), 2)
                print(f"  → Overhead MapReduce ({r['dataset']}) : {ratio}x (normal en local)")
            except: pass
        elif r.get('classic_timeout') and not r.get('mr_timeout'):
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
