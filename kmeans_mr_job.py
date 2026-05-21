"""
kmeans_mr_job.py
Implémentation du paradigme MapReduce avec multiprocessing (sans dépendance externe).
Compatible Python 3.12+.

Modélisation MapReduce :
  MAPPER   : reçoit un chunk de lignes CSV (ou les lignes d'un serveur régional)
             → émet des paires (cluster_id, (somme_partielle, count))
  COMBINER : somme partielle locale par cluster, avant le shuffle (optimisation)
  SHUFFLE  : regroupe toutes les paires par cluster_id (simulé en mémoire)
  REDUCER  : agrège les sommes partielles → nouveau centroïde
"""

import multiprocessing
from utils import euclidean, FEATURE_INDICES


# ─────────────────────────────────────────────
# MAPPER + COMBINER  (exécuté en parallèle sur chaque worker)
# ─────────────────────────────────────────────
def mapper_combiner(args):
    """
    Entrée  : (chunk_lines, centroids) ou (chunk_lines, centroids, norm_params)
              chunk_lines = liste de lignes CSV brutes (un serveur ou un bloc)

    Étape MAP     : pour chaque capteur, calcule la distance aux K centroïdes
                    et émet (cluster_id, point).
    Étape COMBINE : somme locale par cluster_id pour réduire les échanges réseau.

    Sortie  : dict { cluster_id: (partial_sum, count) }
    """
    norm_params = None
    if len(args) == 3:
        chunk_lines, centroids, norm_params = args
        means_n, stds_n = norm_params
    else:
        chunk_lines, centroids = args

    partial = {}

    for line in chunk_lines:
        line = line.strip()
        if not line or line.startswith("sensor_id"):
            continue
        parts = line.split(",")
        try:
            point = [float(parts[i]) for i in FEATURE_INDICES]
        except (ValueError, IndexError):
            continue

        # Normalisation inline si paramètres fournis
        if norm_params is not None:
            point = [(point[d] - means_n[d]) / stds_n[d] for d in range(len(point))]

        # MAP : trouver le centroïde le plus proche
        distances = [euclidean(point, c) for c in centroids]
        cluster_id = distances.index(min(distances))

        # COMBINE local : accumuler la somme partielle
        if cluster_id not in partial:
            partial[cluster_id] = ([0.0] * len(point), 0)
        s, cnt = partial[cluster_id]
        partial[cluster_id] = ([s[d] + point[d] for d in range(len(point))], cnt + 1)

    return partial


# ─────────────────────────────────────────────
# SHUFFLE  (regroupement par clé — simulé en mémoire)
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
    Entrée  : (cluster_id, [(partial_sum, count), ...])
    Agrège les sommes partielles de tous les mappers → nouveau centroïde.
    Sortie  : (cluster_id, new_centroid, total_count)

    Note : pas d'arrondi ici pour préserver la précision numérique entre itérations.
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

    return cluster_id, new_centroid, total_count


# ─────────────────────────────────────────────
# ORCHESTRATEUR : une itération complète
# ─────────────────────────────────────────────
def run_mapreduce_iteration(lines, centroids, n_workers=None, norm_params=None):
    """
    Lance une itération MapReduce complète : Map → Combine → Shuffle → Reduce.

    Paramètres :
        lines       : liste de lignes CSV (mode chunk) OU liste de listes de lignes
                      (mode multi-serveurs : chaque sous-liste = un serveur régional)
        centroids   : centroïdes courants
        n_workers   : nombre de processus parallèles (défaut = nb CPUs)
        norm_params : (means, stds) pour normalisation z-score inline, ou None

    Retourne :
        new_centroids : liste des nouveaux centroïdes
        cluster_counts: dict { cluster_id: count }
    """
    if n_workers is None:
        n_workers = max(1, multiprocessing.cpu_count())

    k = len(centroids)

    # Mode multi-serveurs : lines est une liste de listes (une par serveur)
    if lines and isinstance(lines[0], list):
        chunks = lines
    else:
        chunk_size = max(1, len(lines) // n_workers)
        chunks = [lines[i:i + chunk_size] for i in range(0, len(lines), chunk_size)]

    if norm_params is not None:
        args = [(chunk, centroids, norm_params) for chunk in chunks]
    else:
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

    # Cluster vide → conserver l'ancien centroïde pour éviter None
    for i in range(k):
        if new_centroids[i] is None:
            new_centroids[i] = list(centroids[i])
            cluster_counts[i] = 0

    return new_centroids, cluster_counts
