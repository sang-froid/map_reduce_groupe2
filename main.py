"""
main.py
Point d'entrée unique du projet K-Means MapReduce — Capteurs IoT.
Lance dans l'ordre :
    1. Génération des données
    2. K-means classique (petit dataset)
    3. K-means classique avec timeout (grand dataset)
    4. K-means MapReduce mode chunk
    5. K-means MapReduce mode multi-serveurs
    6. Benchmark comparatif

Usage :
    python main.py
"""

import multiprocessing
multiprocessing.freeze_support()

import os
import sys
import time

# ─────────────────────────────────────────────
def titre(texte):
    print(f"\n{'#'*65}")
    print(f"  {texte}")
    print(f"{'#'*65}\n")

# ─────────────────────────────────────────────
# 1. Génération des données
# ─────────────────────────────────────────────
titre("ÉTAPE 1 — Génération des données IoT simulées")
from generate_data import generate_all
generate_all()

# ─────────────────────────────────────────────
# 2. K-means classique — petit dataset
# ─────────────────────────────────────────────
titre("ÉTAPE 2 — K-means classique (1 500 capteurs)")
from kmeans_classic import load_data, kmeans_classic, cluster_summary
from utils import compute_sse

points = load_data("data/all_sensors.csv")
centroids, assignments, iterations, duration = kmeans_classic(points, k=3, max_iter=20)
cluster_summary(points, assignments, centroids, k=3)

# ─────────────────────────────────────────────
# 3. K-means classique avec timeout — grand dataset
# ─────────────────────────────────────────────
titre("ÉTAPE 3 — K-means classique avec timeout (50 000 capteurs)")
import threading

class TimedOut(Exception): pass

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

large_points = load_data("data/large_sensors.csv")
print(f"  {len(large_points)} capteurs chargés")
print(f"  ⚠ Timeout activé : 20s\n")
try:
    _, _, iters, dur = run_with_timeout(kmeans_classic, (large_points, 3, 20), 20)
    print(f"\n  Terminé en {dur:.3f}s ({iters} itérations)")
except TimedOut as e:
    print(f"\n  ⏱ {e}")
    print("  → Sur 50 000 points, l'algo classique est trop lent. MapReduce s'impose.")

# ─────────────────────────────────────────────
# 4. K-means MapReduce — mode chunk
# ─────────────────────────────────────────────
titre("ÉTAPE 4 — K-means MapReduce mode chunk (all_sensors.csv)")
from kmeans_mapreduce import run_kmeans_mapreduce, print_summary

centroids_mr, counts_mr, iters_mr, dur_mr = run_kmeans_mapreduce(
    "data/all_sensors.csv", k=3, max_iter=10
)
print_summary(centroids_mr, counts_mr)

# ─────────────────────────────────────────────
# 5. K-means MapReduce — mode multi-serveurs
# ─────────────────────────────────────────────
titre("ÉTAPE 5 — K-means MapReduce mode multi-serveurs (5 serveurs régionaux)")
from kmeans_mapreduce import run_kmeans_mapreduce_distributed

SERVER_FILES = [
    "data/server_nord.csv",
    "data/server_centre.csv",
    "data/server_sud.csv",
    "data/server_ouest.csv",
    "data/server_est.csv",
]

centroids_dist, counts_dist, iters_dist, dur_dist = run_kmeans_mapreduce_distributed(
    SERVER_FILES, k=3, max_iter=10
)
print_summary(centroids_dist, counts_dist)

# ─────────────────────────────────────────────
# 6. Benchmark comparatif
# ─────────────────────────────────────────────
titre("ÉTAPE 6 — Benchmark comparatif")
from benchmark import run_benchmark

run_benchmark()

# ─────────────────────────────────────────────
print(f"\n{'='*65}")
print("  TOUT EST TERMINÉ")
print("  Résultats disponibles dans le dossier results/")
print(f"{'='*65}\n")
