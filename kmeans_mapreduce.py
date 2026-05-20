"""
kmeans_mapreduce.py
Orchestre les itérations K-means MapReduce (multiprocessing).

Usage:
    python kmeans_mapreduce.py data/all_sensors.csv --k 3 --max-iter 10
"""

import math
import json
import os
import sys
import time
import csv
import random
import argparse
import multiprocessing

from kmeans_mr_job import run_mapreduce_iteration

FEATURES = ["temperature", "humidity", "vibration", "network_traffic"]
FEATURE_INDICES = [4, 5, 6, 7]

def euclidean(a, b):
    return math.sqrt(sum((x - y) ** 2 for x, y in zip(a, b)))

def initialize_centroids(lines, k, seed=42):
    """Choisit k points aléatoires comme centroïdes initiaux."""
    random.seed(seed)
    points = []
    for line in lines:
        line = line.strip()
        if not line or line.startswith("sensor_id"):
            continue
        parts = line.split(",")
        try:
            points.append([float(parts[i]) for i in FEATURE_INDICES])
        except (ValueError, IndexError):
            continue
    return [list(p) for p in random.sample(points, k)]

def has_converged(old, new, tol=1e-4):
    return all(euclidean(o, n) < tol for o, n in zip(old, new))

def run_kmeans_mapreduce(input_file, k=3, max_iter=20, tol=1e-4, n_workers=None):
    if n_workers is None:
        n_workers = max(1, multiprocessing.cpu_count())

    print(f"\n{'='*55}")
    print(f"  K-MEANS MAPREDUCE (multiprocessing)")
    print(f"  Fichier  : {input_file}")
    print(f"  K={k}  max_iter={max_iter}  workers={n_workers}")
    print(f"{'='*55}\n")

    # Lecture des lignes (partagées entre workers)
    with open(input_file, "r") as f:
        lines = f.readlines()

    centroids = initialize_centroids(lines, k)
    print("Centroïdes initiaux :")
    for i, c in enumerate(centroids):
        print(f"  C{i}: {[round(v, 2) for v in c]}")
    print()

    cluster_counts = {}
    start = time.time()

    for it in range(1, max_iter + 1):
        iter_start = time.time()

        new_centroids, cluster_counts = run_mapreduce_iteration(
            lines, centroids, n_workers=n_workers
        )

        iter_time = time.time() - iter_start
        print(f"  Itération {it:2d} | durée : {iter_time:.3f}s", end="")

        if has_converged(centroids, new_centroids, tol):
            centroids = new_centroids
            print(f"  ✓ Convergence !")
            break
        centroids = new_centroids
        print()

    duration = time.time() - start
    print(f"\n  Durée totale : {duration:.3f}s  ({it} itérations)")
    return centroids, cluster_counts, it, duration

def print_summary(centroids, cluster_counts):
    print(f"\n─── Résumé des clusters ───")
    for i, centroid in enumerate(centroids):
        vals = dict(zip(FEATURES, [round(v, 2) for v in centroid]))
        count = cluster_counts.get(i, 0)
        print(f"  Cluster {i} ({count} capteurs) : {vals}")

def main():
    parser = argparse.ArgumentParser(description="K-means MapReduce (multiprocessing)")
    parser.add_argument("input",       help="Fichier CSV d'entrée")
    parser.add_argument("--k",         type=int, default=3)
    parser.add_argument("--max-iter",  type=int, default=10)
    parser.add_argument("--workers",   type=int, default=None)
    parser.add_argument("--output",    default="results/mapreduce_result.json")
    args = parser.parse_args()

    centroids, cluster_counts, iterations, duration = run_kmeans_mapreduce(
        input_file=args.input, k=args.k, max_iter=args.max_iter, n_workers=args.workers
    )
    print_summary(centroids, cluster_counts)

    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    result = {
        "algorithm": "mapreduce_multiprocessing",
        "n_points": sum(cluster_counts.values()),
        "k": args.k,
        "iterations": iterations,
        "duration_sec": round(duration, 4),
        "centroids": [[round(v, 4) for v in c] for c in centroids],
        "cluster_counts": {str(k): v for k, v in cluster_counts.items()},
    }
    with open(args.output, "w") as f:
        json.dump(result, f, indent=2)
    print(f"\n  Résultats sauvegardés → {args.output}")
    print(f"{'='*55}\n")

if __name__ == "__main__":
    main()
