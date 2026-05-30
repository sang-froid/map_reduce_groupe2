"""
simulation_comparison.py
Simulation de montée en charge : K-means classique vs MapReduce distribué.

Scénario :
    On fait grandir le dataset par paliers (1k → 2M capteurs IoT).
    À chaque palier, on tente l'algorithme classique avec un timeout.
    Passé ce seuil, seul le MapReduce distribué peut terminer.

Usage :
    python simulation_comparison.py               # simulation complète (longue)
    python simulation_comparison.py --dashboard   # version rapide pour le dashboard
"""

import multiprocessing
multiprocessing.freeze_support()

import argparse
import random
import math
import time
import threading
import os
import sys
import json

from kmeans_mr_job import run_mapreduce_iteration
from utils import FEATURES, euclidean, kmeans_plus_plus

# ─────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────
TIMEOUT_S    = 15          # secondes avant abandon du classique
K            = 3           # nombre de clusters
MAX_ITER     = 10          # itérations max
N_SERVERS    = 5           # serveurs régionaux simulés
SEED         = 42

# Paliers de données testés
DATASET_SIZES = [1_000, 5_000, 20_000, 100_000, 500_000, 2_000_000]

# Mode dashboard : paliers plus légers, timeout plus court pour la démo
DASHBOARD_SIZES   = [1_500, 30_000, 100_000, 300_000]
DASHBOARD_TIMEOUT = 12     # secondes — 300k capteurs dépasse ce seuil

# 3 profils de comportement IoT (temp, hum, vibration, trafic réseau)
PROFILES = [
    (35.0, 2.0,  80.0, 5.0,  0.5, 0.1,  120.0, 20.0),   # chaud & humide
    (22.0, 3.0,  45.0, 8.0,  2.5, 0.5,  500.0, 80.0),   # urbain actif
    (28.0, 2.5,  60.0, 6.0,  1.0, 0.2,  250.0, 40.0),   # suburbain modéré
]


# ─────────────────────────────────────────────────────────────────────
# Génération de données en mémoire (pas de fichier disque)
# ─────────────────────────────────────────────────────────────────────
def generate_points(n, seed=SEED):
    random.seed(seed)
    pts = []
    for _ in range(n):
        p = random.choice(PROFILES)
        pts.append([
            random.gauss(p[0], p[1]),
            max(0, min(100, random.gauss(p[2], p[3]))),
            max(0, random.gauss(p[4], p[5])),
            max(0, random.gauss(p[6], p[7])),
        ])
    return pts


def points_to_csv_lines(points):
    """Convertit des points en lignes CSV brutes (format attendu par le MapReduce)."""
    lines = []
    for i, p in enumerate(points):
        lines.append(f"{i},region,0.0,0.0,{p[0]},{p[1]},{p[2]},{p[3]}\n")
    return lines


def split_into_server_chunks(points, n_servers):
    """Répartit les points sur n_servers serveurs régionaux."""
    chunk_size = max(1, len(points) // n_servers)
    chunks = []
    for i in range(0, len(points), chunk_size):
        chunks.append(points_to_csv_lines(points[i:i + chunk_size]))
    return chunks[:n_servers]  # exactement n_servers chunks


# ─────────────────────────────────────────────────────────────────────
# K-means classique séquentiel (pur Python)
# ─────────────────────────────────────────────────────────────────────
def assign_clusters_classic(points, centroids):
    k = len(centroids)
    return [min(range(k), key=lambda j: euclidean(points[i], centroids[j]))
            for i in range(len(points))]


def update_centroids_classic(points, assignments, k):
    dim = len(points[0])
    sums = [[0.0] * dim for _ in range(k)]
    counts = [0] * k
    for p, a in zip(points, assignments):
        for d in range(dim):
            sums[a][d] += p[d]
        counts[a] += 1
    return [
        [sums[i][d] / counts[i] for d in range(dim)] if counts[i] > 0 else sums[i]
        for i in range(k)
    ]


def has_converged(old, new, tol=1e-4):
    return all(euclidean(o, n) < tol for o, n in zip(old, new))


def kmeans_classic_timed(points, k=K, max_iter=MAX_ITER):
    centroids = kmeans_plus_plus(points, k, seed=SEED)
    start = time.time()
    for it in range(1, max_iter + 1):
        assigns = assign_clusters_classic(points, centroids)
        new_c = update_centroids_classic(points, assigns, k)
        if has_converged(centroids, new_c):
            centroids = new_c
            break
        centroids = new_c
    return centroids, time.time() - start, it


# ─────────────────────────────────────────────────────────────────────
# Exécution avec timeout (threading, cross-platform)
# ─────────────────────────────────────────────────────────────────────
class _TimedOut(Exception):
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
        raise _TimedOut()
    if error[0]:
        raise error[0]
    return result[0]


# ─────────────────────────────────────────────────────────────────────
# K-means MapReduce distribué
# ─────────────────────────────────────────────────────────────────────
def kmeans_mapreduce_distributed(points, k=K, max_iter=MAX_ITER, n_servers=N_SERVERS):
    server_chunks = split_into_server_chunks(points, n_servers)
    all_lines = [ln for chunk in server_chunks for ln in chunk]

    # Initialisation K-means++ sur un échantillon de 2 000 pts max (rapide)
    sample = points[:2000] if len(points) > 2000 else points
    centroids = kmeans_plus_plus(sample, k, seed=SEED)

    start = time.time()
    for it in range(1, max_iter + 1):
        new_centroids, cluster_counts = run_mapreduce_iteration(
            server_chunks, centroids, n_workers=n_servers
        )
        if has_converged(centroids, new_centroids):
            centroids = new_centroids
            break
        centroids = new_centroids

    return centroids, cluster_counts, time.time() - start, it


# ─────────────────────────────────────────────────────────────────────
# Affichage
# ─────────────────────────────────────────────────────────────────────
BOLD  = "\033[1m"
RED   = "\033[91m"
GREEN = "\033[92m"
YELLOW= "\033[93m"
CYAN  = "\033[96m"
RESET = "\033[0m"

WIDTH = 72


def bar(label, value, max_val, width=30, color=GREEN):
    filled = int(round(value / max_val * width)) if max_val > 0 else 0
    filled = min(filled, width)
    return f"{color}{'█' * filled}{'░' * (width - filled)}{RESET}  {label}"


def fmt_time(secs, timed_out=False):
    if timed_out:
        return f"{RED}> {TIMEOUT_S}s  TIMEOUT{RESET}"
    if secs < 1:
        return f"{GREEN}{secs*1000:.0f} ms{RESET}"
    return f"{YELLOW}{secs:.2f}s{RESET}"


def print_section(title):
    print(f"\n{BOLD}{'─'*WIDTH}{RESET}")
    print(f"{BOLD}  {title}{RESET}")
    print(f"{BOLD}{'─'*WIDTH}{RESET}")


# ─────────────────────────────────────────────────────────────────────
# Simulation principale
# ─────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dashboard", action="store_true",
                        help="Mode rapide pour le dashboard (paliers réduits)")
    args = parser.parse_args()

    timeout_s = DASHBOARD_TIMEOUT if args.dashboard else TIMEOUT_S
    sizes     = DASHBOARD_SIZES   if args.dashboard else DATASET_SIZES

    cpu_count = multiprocessing.cpu_count()

    print(f"\n{'='*WIDTH}")
    print(f"{BOLD}  SIMULATION — K-MEANS CLASSIQUE vs MAPREDUCE DISTRIBUÉ{RESET}")
    print(f"  Machine    : {cpu_count} CPUs logiques")
    print(f"  Timeout    : {timeout_s}s (abandon du classique au-delà)")
    print(f"  Clusters K : {K}   |   Serveurs régionaux : {N_SERVERS}")
    print(f"  Données    : température, humidité, vibration, trafic réseau")
    if args.dashboard:
        print(f"  Mode       : DASHBOARD — paliers {[f'{n:,}' for n in sizes]}")
    print(f"{'='*WIDTH}\n")

    rows = []

    for n in sizes:
        label = f"{n:>9,} capteurs"
        print_section(f"Dataset : {n:,} capteurs IoT")

        # ── Génération des données ────────────────────────────────────
        t0 = time.time()
        print(f"  Génération des données... ", end="", flush=True)
        points = generate_points(n)
        print(f"{time.time()-t0:.2f}s")

        # ── Classique ────────────────────────────────────────────────
        print(f"\n  {BOLD}[1/2] K-MEANS CLASSIQUE (séquentiel, 1 seul CPU){RESET}")
        print(f"        Timeout : {timeout_s}s — au-delà l'algo est abandonné")
        classic_time = None
        classic_iters = None
        timed_out = False

        try:
            res = run_with_timeout(kmeans_classic_timed, (points, K, MAX_ITER), timeout_s)
            centroids_c, classic_time, classic_iters = res
            print(f"        Résultat : {fmt_time(classic_time)}  "
                  f"({classic_iters} itérations)")
            for i, c in enumerate(centroids_c):
                vals = dict(zip(FEATURES, [round(v, 1) for v in c]))
                print(f"          C{i}: {vals}")
        except _TimedOut:
            timed_out = True
            classic_time = timeout_s
            print(f"        {RED}{BOLD}⏱  TIMEOUT après {timeout_s}s — algorithme trop lent !{RESET}")
            print(f"        {RED}→ Impossible de clusteriser {n:,} capteurs sur un seul nœud.{RESET}")

        # ── MapReduce distribué ───────────────────────────────────────
        print(f"\n  {BOLD}[2/2] K-MEANS MAPREDUCE DISTRIBUÉ ({N_SERVERS} serveurs){RESET}")
        print(f"        MAP local/serveur → SHUFFLE → REDUCE global")
        t0 = time.time()
        centroids_mr, counts_mr, mr_time, mr_iters = kmeans_mapreduce_distributed(
            points, k=K, max_iter=MAX_ITER, n_servers=N_SERVERS
        )
        print(f"        Résultat : {fmt_time(mr_time)}  ({mr_iters} itérations)")
        for i, c in enumerate(centroids_mr):
            vals = dict(zip(FEATURES, [round(v, 1) for v in c]))
            cnt = counts_mr.get(i, 0)
            print(f"          C{i} ({cnt:>7,} capteurs): {vals}")

        # ── Comparaison locale ────────────────────────────────────────
        print(f"\n  {BOLD}Comparaison :{RESET}")
        max_t = max(classic_time, mr_time) if classic_time else mr_time
        print(f"    Classique  : {bar(fmt_time(classic_time, timed_out), classic_time, max_t, color=RED if timed_out else YELLOW)}")
        print(f"    MapReduce  : {bar(fmt_time(mr_time), mr_time, max_t, color=GREEN)}")

        if not timed_out:
            speedup = classic_time / mr_time
            if speedup > 1:
                print(f"\n    {GREEN}→ MapReduce {speedup:.1f}× plus rapide{RESET}")
            else:
                overhead = mr_time / classic_time
                print(f"\n    {YELLOW}→ Overhead MapReduce : {overhead:.1f}× "
                      f"(normal à petite échelle — processus parallèles coûtent leur lancement){RESET}")
        else:
            print(f"\n    {RED}{BOLD}→ Classique : ÉCHEC (timeout){RESET}  "
                  f"{GREEN}{BOLD}MapReduce : SUCCÈS en {mr_time:.2f}s{RESET}")

        rows.append({
            "n": n,
            "classic_s": round(classic_time, 3) if classic_time is not None else timeout_s,
            "mr_s": round(mr_time, 3),
            "timed_out": timed_out,
            "classic_iters": classic_iters,
            "mr_iters": mr_iters,
        })

    # ─────────────────────────────────────────────────────────────────
    # Tableau récapitulatif final
    # ─────────────────────────────────────────────────────────────────
    print(f"\n\n{'='*WIDTH}")
    print(f"{BOLD}  RÉCAPITULATIF — MONTÉE EN CHARGE{RESET}")
    print(f"{'='*WIDTH}")
    print(f"  {'Capteurs':>12}  {'Classique':>18}  {'MapReduce':>12}  {'Verdict':}")
    print(f"  {'─'*12}  {'─'*18}  {'─'*12}  {'─'*20}")

    for r in rows:
        n = r["n"]
        mr = f"{r['mr_s']*1000:.0f} ms" if r['mr_s'] < 1 else f"{r['mr_s']:.2f}s"

        if r["timed_out"]:
            cl = f"> {TIMEOUT_S}s TIMEOUT"
            verdict = f"{RED}Classique KO / MR OK{RESET}"
        else:
            cl_s = r['classic_s']
            cl = f"{cl_s*1000:.0f} ms" if cl_s < 1 else f"{cl_s:.2f}s"
            ratio = r['classic_s'] / r['mr_s']
            if ratio > 1:
                verdict = f"{GREEN}MR {ratio:.1f}× plus rapide{RESET}"
            else:
                verdict = f"{YELLOW}Overhead ×{1/ratio:.1f}{RESET}"

        print(f"  {n:>12,}  {cl:>18}  {mr:>12}  {verdict}")

    print(f"{'='*WIDTH}")
    print(f"""
{BOLD}  CONCLUSION :{RESET}
  • À petite échelle (< 20k pts) : le classique est légèrement plus rapide
    car le démarrage des processus MapReduce a un coût fixe.
  • À grande échelle (> 100k pts) : MapReduce devient nettement supérieur
    grâce au traitement en parallèle sur les {N_SERVERS} serveurs régionaux.
  • Au-delà du timeout ({timeout_s}s) : le classique abandonne, MapReduce termine.
  • En production IoT réel, la parallélisation est encore plus efficace
    car chaque serveur régional traite ses données LOCALEMENT (pas de
    transfert des données brutes, seulement des sommes partielles).
""")

    # ── Sauvegarde JSON des résultats ─────────────────────────────────
    os.makedirs("results", exist_ok=True)
    payload = {
        "timeout_s": timeout_s,
        "k": K,
        "n_servers": N_SERVERS,
        "cpu_count": multiprocessing.cpu_count(),
        "results": rows,
    }
    out = os.path.join("results", "simulation_result.json")
    with open(out, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
    print(f"  Résultats JSON sauvegardés → {out}\n")


if __name__ == "__main__":
    main()
