"""
utils.py — Fonctions et constantes partagées entre les modules K-means.
"""
import math
import csv
import random

FEATURES = ["temperature", "humidity", "vibration", "network_traffic"]
FEATURE_INDICES = [4, 5, 6, 7]  # colonnes CSV : sensor_id, region, lat, lon, temp, hum, vib, net


def euclidean(a, b):
    return math.sqrt(sum((x - y) ** 2 for x, y in zip(a, b)))


def load_data(filepath):
    """Charge un fichier CSV → liste de vecteurs de features."""
    points = []
    with open(filepath, newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                points.append([float(row[feat]) for feat in FEATURES])
            except (ValueError, KeyError):
                continue
    return points


def normalize(points):
    """
    Normalisation z-score par feature (moyenne=0, écart-type=1).
    Indispensable : network_traffic (~500 kbps) domine sinon temperature (~30 °C).
    Retourne (points_normalisés, means, stds).
    """
    dim = len(points[0])
    n = len(points)
    means = [sum(p[d] for p in points) / n for d in range(dim)]
    stds = [
        math.sqrt(sum((p[d] - means[d]) ** 2 for p in points) / n) or 1.0
        for d in range(dim)
    ]
    normalized = [[(p[d] - means[d]) / stds[d] for d in range(dim)] for p in points]
    return normalized, means, stds


def denormalize_centroids(centroids, means, stds):
    """Reconvertit des centroïdes normalisés en valeurs d'origine."""
    return [[c[d] * stds[d] + means[d] for d in range(len(c))] for c in centroids]


def compute_sse(points, assignments, centroids):
    """Somme des erreurs quadratiques (inertie) — mesure la qualité des clusters."""
    return sum(euclidean(p, centroids[a]) ** 2 for p, a in zip(points, assignments))


def kmeans_plus_plus(points, k, seed=42):
    """
    Initialisation K-means++ : choisit les centroïdes initiaux proportionnellement
    à la distance² au centre le plus proche déjà choisi.
    Évite les mauvaises convergences de l'initialisation purement aléatoire.
    """
    random.seed(seed)
    centroids = [list(random.choice(points))]
    for _ in range(k - 1):
        dists = [min(euclidean(p, c) ** 2 for c in centroids) for p in points]
        total = sum(dists)
        probs = [d / total for d in dists]
        r = random.random()
        cumul = 0.0
        for idx, prob in enumerate(probs):
            cumul += prob
            if r <= cumul:
                centroids.append(list(points[idx]))
                break
    return centroids
