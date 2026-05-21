"""
kmeans_mapreduce.py
Orchestre les itérations K-means MapReduce (multiprocessing).

Deux modes :
  - Mode chunk     : découpe un seul grand fichier CSV en blocs parallèles.
  - Mode distribué : chaque fichier CSV régional est assigné à un worker distinct
                     (simulation fidèle du scénario IoT — données sur serveurs régionaux).

Usage:
    # Mode chunk (un seul fichier)
    python kmeans_mapreduce.py data/all_sensors.csv --k 3 --max-iter 10

    # Mode distribué (5 serveurs régionaux)
    python kmeans_mapreduce.py --servers data/server_nord.csv data/server_centre.csv \\
        data/server_sud.csv data/server_ouest.csv data/server_est.csv --k 3

    # Avec normalisation z-score (recommandé)
    python kmeans_mapreduce.py data/all_sensors.csv --k 3 --normalize
"""

import json
import os
import time
import argparse
import multiprocessing

from kmeans_mr_job import run_mapreduce_iteration
from utils import (
    FEATURES, FEATURE_INDICES, euclidean, load_data,
    normalize, denormalize_centroids, compute_sse, kmeans_plus_plus,
)


def has_converged(old, new, tol=1e-4):
    return all(euclidean(o, n) < tol for o, n in zip(old, new))


def load_lines(filepath):
    with open(filepath, "r") as f:
        return f.readlines()


def initialize_centroids_from_lines(lines, k, seed=42):
    """Parse les lignes CSV brutes et initialise les centroïdes via K-means++."""
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
    return kmeans_plus_plus(points, k, seed=seed)


# ─────────────────────────────────────────────
# MODE CHUNK  (un seul grand fichier découpé)
# ─────────────────────────────────────────────
def run_kmeans_mapreduce(input_file, k=3, max_iter=20, tol=1e-4,
                         n_workers=None, normalize_data=False):
    """
    K-means MapReduce en mode chunk.
    Le fichier CSV est découpé en n_workers blocs traités en parallèle.
    """
    if n_workers is None:
        n_workers = max(1, multiprocessing.cpu_count())

    print(f"\n{'='*60}")
    print(f"  K-MEANS MAPREDUCE — Mode chunk")
    print(f"  Fichier   : {input_file}")
    print(f"  K={k}  max_iter={max_iter}  workers={n_workers}  "
          f"normalize={normalize_data}")
    print(f"{'='*60}\n")

    lines = load_lines(input_file)
    centroids = initialize_centroids_from_lines(lines, k)

    norm_params = None
    means, stds = None, None
    if normalize_data:
        points = load_data(input_file)
        _, means, stds = normalize(points)
        centroids = [[(c[d] - means[d]) / stds[d] for d in range(len(c))] for c in centroids]
        norm_params = (means, stds)
        print("Normalisation z-score activée.\n")

    print("Centroïdes initiaux (K-means++) :")
    for i, c in enumerate(centroids):
        print(f"  C{i}: {[round(v, 2) for v in c]}")
    print()

    cluster_counts = {}
    start = time.time()

    for it in range(1, max_iter + 1):
        iter_start = time.time()
        new_centroids, cluster_counts = run_mapreduce_iteration(
            lines, centroids, n_workers=n_workers, norm_params=norm_params
        )
        iter_time = time.time() - iter_start
        print(f"  Itération {it:2d} | durée : {iter_time:.3f}s", end="")
        if has_converged(centroids, new_centroids, tol):
            centroids = new_centroids
            print("  ✓ Convergence !")
            break
        centroids = new_centroids
        print()

    duration = time.time() - start
    print(f"\n  Durée totale : {duration:.3f}s  ({it} itérations)")

    if normalize_data:
        centroids = denormalize_centroids(centroids, means, stds)

    return centroids, cluster_counts, it, duration


# ─────────────────────────────────────────────
# MODE DISTRIBUÉ  (un fichier par serveur régional)
# ─────────────────────────────────────────────
def run_kmeans_mapreduce_distributed(server_files, k=3, max_iter=20, tol=1e-4,
                                     normalize_data=False):
    """
    K-means MapReduce en mode multi-serveurs.
    Chaque fichier CSV = un serveur régional = un worker MapReduce distinct.
    C'est le scénario fidèle au problème : données IoT stockées localement
    sur les serveurs nord, centre, sud, ouest, est.

    MAP    : chaque worker traite ses capteurs locaux → somme partielle.
    REDUCE : agrégation globale → nouveau centroïde.
    """
    n_servers = len(server_files)
    print(f"\n{'='*60}")
    print(f"  K-MEANS MAPREDUCE — Mode multi-serveurs")
    print(f"  Serveurs  : {[os.path.basename(f) for f in server_files]}")
    print(f"  K={k}  max_iter={max_iter}  workers={n_servers}  "
          f"normalize={normalize_data}")
    print(f"{'='*60}\n")

    server_lines = [load_lines(f) for f in server_files]
    all_lines = [line for srv in server_lines for line in srv]

    # Initialisation K-means++ sur la population globale
    centroids = initialize_centroids_from_lines(all_lines, k)

    norm_params = None
    means, stds = None, None
    if normalize_data:
        all_points = []
        for f in server_files:
            all_points.extend(load_data(f))
        _, means, stds = normalize(all_points)
        centroids = [[(c[d] - means[d]) / stds[d] for d in range(len(c))] for c in centroids]
        norm_params = (means, stds)
        print("Normalisation z-score activée.\n")

    print("Centroïdes initiaux (K-means++) :")
    for i, c in enumerate(centroids):
        print(f"  C{i}: {[round(v, 2) for v in c]}")
    print()

    cluster_counts = {}
    start = time.time()

    for it in range(1, max_iter + 1):
        iter_start = time.time()
        # Chaque serveur = un chunk distinct → un worker MapReduce
        new_centroids, cluster_counts = run_mapreduce_iteration(
            server_lines, centroids, n_workers=n_servers, norm_params=norm_params
        )
        iter_time = time.time() - iter_start
        print(f"  Itération {it:2d} | durée : {iter_time:.3f}s", end="")
        if has_converged(centroids, new_centroids, tol):
            centroids = new_centroids
            print("  ✓ Convergence !")
            break
        centroids = new_centroids
        print()

    duration = time.time() - start
    total = sum(cluster_counts.values())
    print(f"\n  Durée totale : {duration:.3f}s  ({it} itérations)  ({total} capteurs)")

    if normalize_data:
        centroids = denormalize_centroids(centroids, means, stds)

    return centroids, cluster_counts, it, duration


# ─────────────────────────────────────────────
# Affichage
# ─────────────────────────────────────────────
def print_summary(centroids, cluster_counts):
    print("\n─── Résumé des clusters ───")
    for i, centroid in enumerate(centroids):
        vals = dict(zip(FEATURES, [round(v, 2) for v in centroid]))
        count = cluster_counts.get(i, 0)
        print(f"  Cluster {i} ({count:>5} capteurs) : {vals}")


# ─────────────────────────────────────────────
# Point d'entrée
# ─────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="K-means MapReduce (multiprocessing)")
    parser.add_argument("input",        nargs="?",
                        help="Fichier CSV unique (mode chunk)")
    parser.add_argument("--servers",    nargs="+",
                        help="Fichiers CSV par serveur régional (mode distribué)")
    parser.add_argument("--k",          type=int, default=3)
    parser.add_argument("--max-iter",   type=int, default=10)
    parser.add_argument("--workers",    type=int, default=None,
                        help="Nombre de workers (mode chunk uniquement)")
    parser.add_argument("--normalize",  action="store_true",
                        help="Normaliser les features (z-score) avant le clustering")
    parser.add_argument("--output",     default="results/mapreduce_result.json")
    args = parser.parse_args()

    if args.servers:
        centroids, cluster_counts, iterations, duration = run_kmeans_mapreduce_distributed(
            server_files=args.servers, k=args.k, max_iter=args.max_iter,
            normalize_data=args.normalize,
        )
        algo = "mapreduce_distributed"
    elif args.input:
        centroids, cluster_counts, iterations, duration = run_kmeans_mapreduce(
            input_file=args.input, k=args.k, max_iter=args.max_iter,
            n_workers=args.workers, normalize_data=args.normalize,
        )
        algo = "mapreduce_chunk"
    else:
        parser.error("Fournir un fichier d'entrée ou --servers.")

    print_summary(centroids, cluster_counts)

    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    result = {
        "algorithm": algo,
        "n_points": sum(cluster_counts.values()),
        "k": args.k,
        "iterations": iterations,
        "duration_sec": round(duration, 4),
        "centroids": [[round(v, 4) for v in c] for c in centroids],
        "cluster_counts": {str(cid): v for cid, v in cluster_counts.items()},
    }
    with open(args.output, "w") as f:
        json.dump(result, f, indent=2)
    print(f"\n  Résultats sauvegardés → {args.output}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
