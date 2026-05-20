"""
kmeans_classic.py
Implémentation classique (séquentielle) de K-means sur un seul nœud.
Inclut un mécanisme de timeout pour simuler la limite d'un ordinateur ordinaire
face à de grands volumes de données.

Usage:
    python kmeans_classic.py --input data/all_sensors.csv --k 3 --max-iter 10
    python kmeans_classic.py --input data/large_sensors.csv --k 3 --timeout 5
"""

import csv
import math
import time
import random
import argparse
import signal
import sys
import json
import os

# ─────────────────────────────────────────────
# Gestion du timeout (UNIX uniquement)
# ─────────────────────────────────────────────
class TimeoutError(Exception):
    pass

def _timeout_handler(signum, frame):
    raise TimeoutError("⏱ Timeout atteint ! L'algorithme classique est trop lent sur ce volume.")

def set_timeout(seconds):
    """Active le timeout (Linux/Mac uniquement)."""
    if hasattr(signal, 'SIGALRM'):
        signal.signal(signal.SIGALRM, _timeout_handler)
        signal.alarm(seconds)

def cancel_timeout():
    if hasattr(signal, 'SIGALRM'):
        signal.alarm(0)

# ─────────────────────────────────────────────
# Chargement des données
# ─────────────────────────────────────────────
FEATURES = ["temperature", "humidity", "vibration", "network_traffic"]

def load_data(filepath):
    """Charge les données CSV et retourne une liste de vecteurs de features."""
    points = []
    with open(filepath, newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            vec = [float(row[feat]) for feat in FEATURES]
            points.append(vec)
    return points

# ─────────────────────────────────────────────
# Fonctions K-means
# ─────────────────────────────────────────────
def euclidean(a, b):
    return math.sqrt(sum((x - y) ** 2 for x, y in zip(a, b)))

def initialize_centroids(points, k, seed=42):
    """Initialisation aléatoire des centroïdes (K-means++)  simplifié."""
    random.seed(seed)
    return [list(p) for p in random.sample(points, k)]

def assign_clusters(points, centroids):
    """Étape MAP : associe chaque point au centroïde le plus proche."""
    assignments = []
    for p in points:
        distances = [euclidean(p, c) for c in centroids]
        assignments.append(distances.index(min(distances)))
    return assignments

def update_centroids(points, assignments, k):
    """Étape REDUCE : recalcule les centroïdes."""
    dim = len(points[0])
    sums = [[0.0] * dim for _ in range(k)]
    counts = [0] * k
    for p, a in zip(points, assignments):
        for d in range(dim):
            sums[a][d] += p[d]
        counts[a] += 1
    new_centroids = []
    for i in range(k):
        if counts[i] > 0:
            new_centroids.append([s / counts[i] for s in sums[i]])
        else:
            new_centroids.append(sums[i])
    return new_centroids

def has_converged(old, new, tol=1e-4):
    return all(euclidean(o, n) < tol for o, n in zip(old, new))

def kmeans_classic(points, k=3, max_iter=20, tol=1e-4):
    """
    K-means classique séquentiel.
    Retourne (centroids, assignments, iterations, duration_sec)
    """
    centroids = initialize_centroids(points, k)
    assignments = []
    start = time.time()

    for it in range(1, max_iter + 1):
        iter_start = time.time()
        assignments = assign_clusters(points, centroids)
        new_centroids = update_centroids(points, assignments, k)
        iter_time = time.time() - iter_start
        print(f"  Itération {it:2d} | durée : {iter_time:.3f}s", end="")
        if has_converged(centroids, new_centroids, tol):
            centroids = new_centroids
            print(f"  ✓ Convergence !")
            break
        centroids = new_centroids
        print()

    duration = time.time() - start
    return centroids, assignments, it, duration

# ─────────────────────────────────────────────
# Résumé des clusters
# ─────────────────────────────────────────────
def cluster_summary(points, assignments, centroids, k):
    counts = [0] * k
    for a in assignments:
        counts[a] += 1
    print("\n─── Résumé des clusters ───")
    for i, (centroid, count) in enumerate(zip(centroids, counts)):
        vals = dict(zip(FEATURES, [round(v, 2) for v in centroid]))
        print(f"  Cluster {i} ({count} capteurs) : {vals}")

# ─────────────────────────────────────────────
# Point d'entrée
# ─────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="K-means classique séquentiel")
    parser.add_argument("--input",    default="data/all_sensors.csv")
    parser.add_argument("--k",        type=int, default=3)
    parser.add_argument("--max-iter", type=int, default=20)
    parser.add_argument("--timeout",  type=int, default=0,
                        help="Timeout en secondes (0 = désactivé)")
    parser.add_argument("--output",   default="results/classic_result.json")
    args = parser.parse_args()

    print(f"\n{'='*55}")
    print(f"  K-MEANS CLASSIQUE (séquentiel)")
    print(f"  Fichier : {args.input}")
    print(f"  K={args.k}  max_iter={args.max_iter}  timeout={args.timeout}s")
    print(f"{'='*55}\n")

    print("Chargement des données...")
    t0 = time.time()
    points = load_data(args.input)
    print(f"  {len(points)} capteurs chargés en {time.time()-t0:.3f}s\n")

    if args.timeout > 0:
        print(f"⚠  Timeout activé : {args.timeout}s\n")
        set_timeout(args.timeout)

    timed_out = False
    try:
        centroids, assignments, iterations, duration = kmeans_classic(
            points, k=args.k, max_iter=args.max_iter
        )
        cancel_timeout()
    except TimeoutError as e:
        timed_out = True
        print(f"\n{e}")
        duration = args.timeout
        centroids, assignments, iterations = [], [], 0

    print(f"\n{'='*55}")
    if timed_out:
        print(f"  RÉSULTAT : ÉCHEC (timeout après {duration}s)")
    else:
        print(f"  RÉSULTAT : {iterations} itérations en {duration:.3f}s")
        cluster_summary(points, assignments, centroids, args.k)

    # Sauvegarde JSON
    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    result = {
        "algorithm": "classic",
        "n_points": len(points),
        "k": args.k,
        "iterations": iterations,
        "duration_sec": round(duration, 4),
        "timed_out": timed_out,
        "centroids": [[round(v, 4) for v in c] for c in centroids],
    }
    with open(args.output, "w") as f:
        json.dump(result, f, indent=2)
    print(f"\n  Résultats sauvegardés → {args.output}")
    print(f"{'='*55}\n")

if __name__ == "__main__":
    main()
