"""
kmeans_mr_job.py
Implémentation du paradigme MapReduce avec multiprocessing (sans mrjob).
Compatible Python 3.12+.

Modélisation :
  MAPPER  : reçoit un chunk de lignes CSV, retourne liste de (cluster_id, (point, 1))
  COMBINER: somme partielle locale par cluster (optimisation pré-shuffle)
  SHUFFLE : regroupe par cluster_id (simulé en mémoire)
  REDUCER : calcule le nouveau centroïde depuis les sommes partielles
"""

import math
import multiprocessing
import csv
import io

FEATURE_INDICES = [4, 5, 6, 7]  # temperature, humidity, vibration, network_traffic

def euclidean(a, b):
    return math.sqrt(sum((x - y) ** 2 for x, y in zip(a, b)))

# ─────────────────────────────────────────────
# MAPPER + COMBINER  (exécuté en parallèle)
# ─────────────────────────────────────────────
def mapper_combiner(args):
    """
    Reçoit (chunk_lines, centroids).
    Pour chaque capteur : trouve le cluster le plus proche (MAP).
    Puis somme partielle par cluster (COMBINE).
    Retourne dict { cluster_id: (partial_sum, count) }
    """
    chunk_lines, centroids = args
    partial = {}  # { cluster_id: ([sum_features], count) }

    for line in chunk_lines:
        line = line.strip()
        if not line or line.startswith("sensor_id"):
            continue
        parts = line.split(",")
        try:
            point = [float(parts[i]) for i in FEATURE_INDICES]
        except (ValueError, IndexError):
            continue

        # MAP : associer au centroïde le plus proche
        distances = [euclidean(point, c) for c in centroids]
        cluster_id = distances.index(min(distances))

        # COMBINE local : somme partielle
        if cluster_id not in partial:
            partial[cluster_id] = ([0.0] * len(point), 0)
        s, cnt = partial[cluster_id]
        partial[cluster_id] = ([s[d] + point[d] for d in range(len(point))], cnt + 1)

    return partial


# ─────────────────────────────────────────────
# SHUFFLE  (regroupement par clé)
# ─────────────────────────────────────────────
def shuffle(partial_results, k):
    """
    Regroupe les résultats partiels de tous les mappers par cluster_id.
    Retourne dict { cluster_id: [(partial_sum, count), ...] }
    """
    shuffled = {i: [] for i in range(k)}
    for partial in partial_results:
        for cluster_id, (partial_sum, count) in partial.items():
            shuffled[cluster_id].append((partial_sum, count))
    return shuffled


# ─────────────────────────────────────────────
# REDUCER  (calcul du nouveau centroïde)
# ─────────────────────────────────────────────
def reducer(cluster_id, values):
    """
    Agrège les sommes partielles → nouveau centroïde.
    Retourne (cluster_id, new_centroid, total_count)
    """
    total_sum = None
    total_count = 0
    for partial_sum, count in values:
        if total_sum is None:
            total_sum = list(partial_sum)
        else:
            total_sum = [a + b for a, b in zip(total_sum, partial_sum)]
        total_count += count

    if total_count > 0:
        new_centroid = [s / total_count for s in total_sum]
    else:
        new_centroid = total_sum or [0.0] * len(FEATURE_INDICES)

    return cluster_id, [round(v, 6) for v in new_centroid], total_count


# ─────────────────────────────────────────────
# ORCHESTRATEUR : une itération complète
# ─────────────────────────────────────────────
def run_mapreduce_iteration(lines, centroids, n_workers=None):
    """
    Lance une itération MapReduce complète :
    Map → Combine → Shuffle → Reduce

    Paramètres :
        lines      : liste de lignes CSV (strings)
        centroids  : liste des centroïdes courants
        n_workers  : nombre de processus parallèles (défaut = nb CPUs)

    Retourne :
        new_centroids : liste des nouveaux centroïdes
        cluster_counts: dict { cluster_id: count }
    """
    if n_workers is None:
        n_workers = max(1, multiprocessing.cpu_count())

    k = len(centroids)

    # Découpage des données en chunks (un par worker)
    chunk_size = max(1, len(lines) // n_workers)
    chunks = [lines[i:i + chunk_size] for i in range(0, len(lines), chunk_size)]
    args = [(chunk, centroids) for chunk in chunks]

    # ── MAP + COMBINE (parallèle) ──────────────
    with multiprocessing.Pool(processes=n_workers) as pool:
        partial_results = pool.map(mapper_combiner, args)

    # ── SHUFFLE ───────────────────────────────
    shuffled = shuffle(partial_results, k)

    # ── REDUCE ────────────────────────────────
    new_centroids = [None] * k
    cluster_counts = {}
    for cluster_id, values in shuffled.items():
        if values:
            cid, centroid, count = reducer(cluster_id, values)
            new_centroids[cid] = centroid
            cluster_counts[cid] = count

    # Gérer les clusters vides
    for i in range(k):
        if new_centroids[i] is None:
            new_centroids[i] = list(centroids[i])
            cluster_counts[i] = 0

    return new_centroids, cluster_counts
