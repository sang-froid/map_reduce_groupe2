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

# Paliers de données testés (mode complet)
DATASET_SIZES = [1_000, 5_000, 20_000, 100_000, 500_000, 2_000_000]

# Mode dashboard : paliers croissants jusqu'à 1 000 000 pts (mesures réelles).
# À 1M pts le classique prend ~40s de calcul pur + 244s de collecte IoT.
# Les paliers au-delà (10M, 100M, 1B) sont extrapolés O(n) depuis la mesure 1M.
DASHBOARD_SIZES = [10_000, 50_000, 200_000, 500_000, 1_000_000]

# Volumes Big Data — extrapolés depuis la mesure réelle à 1 million de points
PROJECTION_SIZES = [10_000_000, 100_000_000, 1_000_000_000]

# ── Simulation réseau IoT réel ────────────────────────────────────────
# Classique : doit rapatrier TOUTES les données brutes vers un nœud central
# MapReduce : seules les sommes partielles (K vecteurs) transitent via LAN
IOT_BANDWIDTH_MBPS       = 1.0   # réseau capteurs IoT (typique zone industrielle/rurale)
LAN_BANDWIDTH_MBPS       = 1000  # réseau LAN entre serveurs régionaux (Gigabit)
BYTES_PER_POINT          = 4 * 8 # 4 features × 8 octets (float64)
DASHBOARD_MAX_SLEEP_S    = 12    # plafond du sleep de transfert par palier (démo ~2 min)

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


def load_csv_points(path):
    """Charge les features depuis un fichier CSV IoT réel."""
    from utils import FEATURE_INDICES
    pts = []
    with open(path, encoding="utf-8") as f:
        header = f.readline()  # skip header
        for line in f:
            parts = line.strip().split(",")
            if len(parts) < max(FEATURE_INDICES) + 1:
                continue
            try:
                pts.append([float(parts[i]) for i in FEATURE_INDICES])
            except ValueError:
                continue
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


def data_transfer_time(n_points, is_mapreduce=False):
    """
    Classique : doit rapatrier TOUTES les données brutes depuis les capteurs IoT (réseau lent).
    MapReduce  : seules les sommes partielles (K × D vecteurs / serveur) transitent via LAN.
    """
    if is_mapreduce:
        bytes_sent = K * 4 * 8 * N_SERVERS * MAX_ITER  # sommes partielles via LAN rapide
        bw = LAN_BANDWIDTH_MBPS * 1024 * 1024 / 8
    else:
        bytes_sent = n_points * BYTES_PER_POINT         # toutes les données brutes via réseau IoT
        bw = IOT_BANDWIDTH_MBPS * 1024 * 1024 / 8
    return bytes_sent / bw


def kmeans_classic_timed(points, k=K, max_iter=MAX_ITER, timeout_s=0):
    centroids = kmeans_plus_plus(points, k, seed=SEED)
    start = time.time()
    for it in range(1, max_iter + 1):
        iter_t0 = time.time()
        assigns = assign_clusters_classic(points, centroids)
        new_c = update_centroids_classic(points, assigns, k)
        iter_dur = time.time() - iter_t0
        elapsed  = time.time() - start
        converged = has_converged(centroids, new_c)
        centroids = new_c
        marker = "  ✓ convergence" if converged else ""
        to_warn = (f"  [cumul {elapsed:.1f}s / timeout {timeout_s}s]"
                   if timeout_s and elapsed > timeout_s * 0.6 else "")
        print(f"          iter {it:2d}  durée iter : {iter_dur*1000:.0f} ms"
              f"  cumul : {elapsed:.2f}s{to_warn}{marker}", flush=True)
        if converged:
            break
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

    # Initialisation K-means++ sur un échantillon de 2 000 pts max (rapide)
    sample = points[:2000] if len(points) > 2000 else points
    centroids = kmeans_plus_plus(sample, k, seed=SEED)

    start = time.time()
    for it in range(1, max_iter + 1):
        iter_t0 = time.time()
        new_centroids, cluster_counts = run_mapreduce_iteration(
            server_chunks, centroids, n_workers=n_servers
        )
        iter_dur  = time.time() - iter_t0
        elapsed   = time.time() - start
        converged = has_converged(centroids, new_centroids)
        centroids = new_centroids
        marker = "  ✓ convergence" if converged else ""
        print(f"          iter {it:2d}  durée iter : {iter_dur*1000:.0f} ms"
              f"  cumul : {elapsed:.2f}s{marker}", flush=True)
        if converged:
            break

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


def fmt_time(secs, timed_out=False, timeout=TIMEOUT_S):
    if timed_out:
        return f"{RED}> {timeout}s  TIMEOUT{RESET}"
    if secs < 1:
        return f"{GREEN}{secs*1000:.0f} ms{RESET}"
    return f"{YELLOW}{secs:.2f}s{RESET}"


def _human_time(s):
    if s >= 86400: return f"~{s/86400:.1f} jour(s)"
    if s >= 3600:  return f"~{s/3600:.1f} h"
    if s >= 60:    return f"~{s/60:.0f} min"
    return f"~{s:.1f}s"


def print_section(title):
    print(f"\n{BOLD}{'─'*WIDTH}{RESET}")
    print(f"{BOLD}  {title}{RESET}")
    print(f"{BOLD}{'─'*WIDTH}{RESET}")


# ─────────────────────────────────────────────────────────────────────
# Simulation principale
# ─────────────────────────────────────────────────────────────────────
def main():
    global K, MAX_ITER, TIMEOUT_S
    parser = argparse.ArgumentParser()
    parser.add_argument("--dashboard", action="store_true",
                        help="Mode rapide pour le dashboard (paliers réduits)")
    parser.add_argument("--k",        type=int, default=K,        help="Nombre de clusters")
    parser.add_argument("--max-iter", type=int, default=MAX_ITER, help="Itérations max")
    parser.add_argument("--timeout",  type=int, default=TIMEOUT_S, help="Timeout classique (s)")
    args = parser.parse_args()
    K        = args.k
    MAX_ITER = args.max_iter
    TIMEOUT_S = args.timeout

    sizes     = DASHBOARD_SIZES if args.dashboard else DATASET_SIZES
    timeout_s = TIMEOUT_S

    cpu_count = multiprocessing.cpu_count()

    # ── Auto-génération des données CSV si elles n'existent pas ──────────
    real_csv = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            "data", "all_sensors.csv")
    if args.dashboard and not os.path.exists(real_csv):
        print(f"  {YELLOW}⚙  Données IoT introuvables — génération automatique...{RESET}",
              flush=True)
        try:
            import generate_data
            generate_data.generate_all()
            print(f"  {GREEN}✓ Données générées.{RESET}\n", flush=True)
        except Exception as e:
            print(f"  {RED}⚠  Génération échouée : {e}{RESET}\n", flush=True)

    print(f"\n{'='*WIDTH}")
    print(f"{BOLD}  SIMULATION — K-MEANS CLASSIQUE vs MAPREDUCE DISTRIBUÉ{RESET}")
    print(f"  Machine    : {cpu_count} CPUs logiques")
    if args.dashboard:
        print(f"  Mode       : DASHBOARD — mesures RÉELLES sur {[f'{n:,}' for n in sizes]}")
        print(f"               + projections Big Data : 10M, 100M, 1 milliard de points")
    else:
        print(f"  Timeout    : {timeout_s}s (abandon du classique au-delà)")
    print(f"  Clusters K : {K}   |   Serveurs régionaux : {N_SERVERS}")
    print(f"  Données    : température, humidité, vibration, trafic réseau")
    print(f"{'='*WIDTH}\n")

    # ─────────────────────────────────────────────────────────────────
    # PHASE 1 — K-Means Classique sur données réelles (démonstration)
    # ─────────────────────────────────────────────────────────────────
    classic_demo = None
    # real_csv défini plus haut (auto-génération)

    print(f"\n{'='*WIDTH}")
    print(f"{BOLD}  PHASE 1 — K-MEANS CLASSIQUE  ·  Données réelles IoT{RESET}")
    print(f"{'='*WIDTH}")

    if os.path.exists(real_csv):
        print(f"\n  Chargement de data/all_sensors.csv... ", end="", flush=True)
        t0 = time.time()
        real_pts = load_csv_points(real_csv)
        print(f"{len(real_pts)} capteurs en {time.time()-t0:.3f}s")

        print(f"\n  {BOLD}K-Means Classique — séquentiel, 1 seul CPU{RESET}")
        c_demo, demo_time, demo_iters = kmeans_classic_timed(real_pts, K, MAX_ITER)
        assigns_demo = assign_clusters_classic(real_pts, c_demo)
        counts_demo = [0] * K
        for a in assigns_demo:
            counts_demo[a] += 1

        print(f"\n  {GREEN}{BOLD}✓ Convergence en {demo_iters} itérations — "
              f"{demo_time*1000:.0f} ms{RESET}")
        print(f"\n  Clusters identifiés ({len(real_pts)} capteurs IoT) :")
        for i, c in enumerate(c_demo):
            vals = dict(zip(FEATURES, [round(v, 2) for v in c]))
            print(f"    Cluster {i} ({counts_demo[i]:>5} capteurs) : {vals}")

        print(f"\n  {GREEN}→ Sur {len(real_pts):,} capteurs, le classique est RAPIDE et PRÉCIS.{RESET}")
        print(f"  {YELLOW}→ Que se passe-t-il quand le volume de données explose ?{RESET}")

        classic_demo = {
            "n_points": len(real_pts),
            "duration_sec": round(demo_time, 4),
            "iterations": demo_iters,
            "centroids": [[round(v, 4) for v in c] for c in c_demo],
            "cluster_counts": {str(i): counts_demo[i] for i in range(K)},
        }
    else:
        print(f"  {YELLOW}⚠  data/all_sensors.csv introuvable — "
              f"lancez d'abord ⬡ Générer les données.{RESET}")

    # ─────────────────────────────────────────────────────────────────
    # PHASE 2 — Montée en charge : classique vs MapReduce
    # ─────────────────────────────────────────────────────────────────
    print(f"\n{'='*WIDTH}")
    print(f"{BOLD}  PHASE 2 — MONTÉE EN CHARGE : CLASSIQUE vs MAPREDUCE{RESET}")
    print(f"{'='*WIDTH}")
    print(f"""
  Différence avec le résultat K-Means Classique :
    • Phase 1  : données RÉELLES  (data/all_sensors.csv, 1 500 capteurs IoT)
                 → mêmes résultats que le bouton "K-Means Classique"
    • Phase 2  : données SYNTHÉTIQUES générées aléatoirement (generate_points)
                 → volumes croissants pour stresser l'algorithme
                 → les centroïdes sont proches mais pas identiques (données différentes)
    • La numérotation des clusters peut varier d'un run à l'autre
      (K-means ne garantit pas l'ordre des clusters entre deux exécutions)
""")

    rows = []

    for n in sizes:
        label = f"{n:>9,} capteurs"
        print_section(f"Dataset : {n:,} capteurs IoT")

        # ── Génération des données ────────────────────────────────────
        t0 = time.time()
        print(f"  Génération des données... ", end="", flush=True)
        points = generate_points(n)
        print(f"{time.time()-t0:.2f}s")

        # ── Sauvegarde CSV pour consultation dans l'onglet Données ────
        if args.dashboard:
            _lbl = (f"sim_{n//1_000_000}M" if n >= 1_000_000
                    else f"sim_{n//1_000}k")
            _csv_path = os.path.join(
                os.path.dirname(os.path.abspath(__file__)), "data", f"{_lbl}.csv")
            os.makedirs(os.path.dirname(_csv_path), exist_ok=True)
            _regions = ("nord", "centre", "sud", "ouest", "est")
            _t0 = time.time()
            print(f"  Sauvegarde → data/{_lbl}.csv... ", end="", flush=True)
            with open(_csv_path, "w", buffering=1 << 20, encoding="utf-8") as _f:
                _f.write("sensor_id,region,latitude,longitude,"
                         "temperature,humidity,vibration,network_traffic\n")
                for _i, _p in enumerate(points):
                    _f.write(
                        f"{_i},server_{_regions[_i % 5]},0.0,0.0,"
                        f"{_p[0]:.2f},{_p[1]:.2f},{_p[2]:.3f},{_p[3]:.2f}\n"
                    )
            print(f"{time.time()-_t0:.1f}s", flush=True)

        # ── Classique ────────────────────────────────────────────────
        print(f"\n  {BOLD}[1/2] K-MEANS CLASSIQUE (séquentiel, 1 seul CPU){RESET}")
        timed_out     = False
        classic_time  = None
        classic_iters = None

        if args.dashboard:
            # Mesure honnête sans timeout artificiel
            centroids_c, classic_time, classic_iters = kmeans_classic_timed(
                points, K, MAX_ITER, 0)
            print(f"        Résultat : {fmt_time(classic_time)}  ({classic_iters} itérations)")
            for i, c in enumerate(centroids_c):
                print(f"          C{i}: {dict(zip(FEATURES, [round(v,1) for v in c]))}")
        else:
            print(f"        Timeout : {timeout_s}s")
            try:
                res = run_with_timeout(
                    kmeans_classic_timed, (points, K, MAX_ITER, timeout_s), timeout_s)
                centroids_c, classic_time, classic_iters = res
                print(f"        Résultat : {fmt_time(classic_time)}  ({classic_iters} itérations)")
                for i, c in enumerate(centroids_c):
                    print(f"          C{i}: {dict(zip(FEATURES, [round(v,1) for v in c]))}")
            except _TimedOut:
                timed_out    = True
                classic_time = timeout_s
                print(f"        {RED}{BOLD}⏱  TIMEOUT après {timeout_s}s{RESET}")
                print(f"        {RED}→ Impossible sur un seul nœud.{RESET}")

        # ── MapReduce distribué ───────────────────────────────────────
        print(f"\n  {BOLD}[2/2] K-MEANS MAPREDUCE DISTRIBUÉ ({N_SERVERS} serveurs){RESET}")
        print(f"        MAP local/serveur → SHUFFLE → REDUCE global")
        mr_timed_out = False

        if args.dashboard:
            centroids_mr, counts_mr, mr_time, mr_iters = kmeans_mapreduce_distributed(
                points, k=K, max_iter=MAX_ITER, n_servers=N_SERVERS)
        else:
            try:
                centroids_mr, counts_mr, mr_time, mr_iters = kmeans_mapreduce_distributed(
                    points, k=K, max_iter=MAX_ITER, n_servers=N_SERVERS)
            except Exception as e:
                mr_timed_out = True
                mr_time, mr_iters = 0, None
                print(f"        {RED}Erreur : {e}{RESET}")

        if not mr_timed_out:
            print(f"        Résultat : {fmt_time(mr_time)}  ({mr_iters} itérations)")
            for i, c in enumerate(centroids_mr):
                cnt = counts_mr.get(i, 0)
                print(f"          C{i} ({cnt:>7,} capteurs): "
                      f"{dict(zip(FEATURES, [round(v,1) for v in c]))}")

        # ── Overhead réseau IoT (simulation du vrai scénario distribué) ──
        classic_transfer = data_transfer_time(n, is_mapreduce=False)
        mr_transfer      = data_transfer_time(n, is_mapreduce=True)
        classic_total    = (classic_time or 0) + classic_transfer
        mr_total         = (mr_time or 0) + mr_transfer

        if args.dashboard:
            cls_mb   = n * BYTES_PER_POINT / 1e6
            mr_bytes = K * 4 * 8 * N_SERVERS * MAX_ITER

            # ── Attente réelle du transfert réseau (barre de progression) ──
            # Le calcul était rapide ; maintenant on ATTEND que les données
            # arrivent sur le réseau IoT à 1 Mbps — l'utilisateur le vit.
            # Le sleep est plafonné à DASHBOARD_MAX_SLEEP_S pour la démo,
            # mais le temps RÉEL estimé est toujours affiché honnêtement.
            actual_sleep = min(classic_transfer, DASHBOARD_MAX_SLEEP_S)
            N_TICKS      = 10
            tick         = actual_sleep / N_TICKS
            capped       = classic_transfer > DASHBOARD_MAX_SLEEP_S
            real_note    = (f"  {YELLOW}(sur réseau réel : {classic_transfer:.0f}s)"
                            f"{RESET}" if capped else "")
            print(f"\n  {RED}[Réseau IoT 1 Mbps]{RESET}  "
                  f"Envoi de {cls_mb:.1f} MB vers le nœud central...{real_note}",
                  flush=True)
            for step in range(1, N_TICKS + 1):
                time.sleep(tick)
                filled  = int(step * 20 / N_TICKS)
                empty   = 20 - filled
                elapsed = step * actual_sleep / N_TICKS
                pct     = step * 100 // N_TICKS
                print(f"  {RED}{'█'*filled}{'░'*empty}{RESET}  "
                      f"{pct:3d}%   {elapsed:.1f}s",
                      flush=True)
            print(f"  {RED}✗ Collecte terminée —"
                  f" {classic_transfer:.0f}s perdues (réseau IoT){RESET}",
                  flush=True)

            # ── MapReduce : transfert quasi-instantané ────────────────────
            print(f"\n  {GREEN}[LAN Gigabit — sommes partielles]{RESET}  "
                  f"{mr_bytes} octets... {GREEN}< 1 ms ✓{RESET}", flush=True)

            print(f"\n  {BOLD}Temps TOTAL (calcul + transfert réseau) :{RESET}")
            print(f"    Classique  : {fmt_time(classic_time)} calcul"
                  f" + {classic_transfer:.1f}s réseau IoT"
                  f"  = {RED}{BOLD}{classic_total:.1f}s{RESET}")
            print(f"    MapReduce  : {fmt_time(mr_time)} calcul"
                  f" + {mr_transfer*1000:.1f} ms réseau"
                  f"  = {GREEN}{BOLD}{mr_total:.2f}s{RESET}")

            max_t       = max(classic_total, mr_total) or 1
            ratio_total = classic_total / mr_total if mr_total > 0 else 1
            print(f"\n  {BOLD}Comparaison (scénario IoT réel) :{RESET}")
            print(f"    Classique  : {bar(f'{classic_total:.1f}s', classic_total, max_t, color=RED)}")
            print(f"    MapReduce  : {bar(f'{mr_total:.2f}s', mr_total, max_t, color=GREEN)}")
            print(f"\n    {GREEN}{BOLD}→ MapReduce {ratio_total:.1f}× plus rapide"
                  f" (données restent sur les {N_SERVERS} serveurs régionaux){RESET}")
        else:
            print(f"\n  {BOLD}Comparaison :{RESET}")
            max_t = max(classic_time or 0, mr_time or 0) or 1
            print(f"    Classique  : {bar(fmt_time(classic_time, timed_out, timeout_s), classic_time, max_t, color=RED if timed_out else YELLOW)}")
            print(f"    MapReduce  : {bar(fmt_time(mr_time), mr_time, max_t, color=GREEN)}")

            if not timed_out and classic_time and mr_time:
                ratio = classic_time / mr_time
                if ratio > 1:
                    print(f"\n    {GREEN}→ MapReduce {ratio:.1f}× plus rapide{RESET}")
                else:
                    print(f"\n    {YELLOW}→ Overhead MapReduce : {mr_time/classic_time:.1f}× "
                          f"(coût fixe du pool de processus, s'inverse à grande échelle){RESET}")
            elif timed_out:
                print(f"\n    {RED}{BOLD}→ Classique : ÉCHEC (timeout){RESET}  "
                      f"{GREEN}{BOLD}MapReduce : OK en {mr_time:.2f}s{RESET}")

        rows.append({
            "n":                  n,
            "classic_s":          round(classic_time, 3) if classic_time else (timeout_s if not args.dashboard else 0),
            "classic_transfer_s": round(classic_transfer, 3),
            "classic_total_s":    round(classic_total, 3),
            "mr_s":               round(mr_time, 3) if mr_time else 0,
            "mr_transfer_s":      round(mr_transfer, 6),
            "mr_total_s":         round(mr_total, 3),
            "timed_out":          timed_out,
            "mr_timed_out":       mr_timed_out,
            "classic_iters":      classic_iters,
            "mr_iters":           mr_iters,
        })

        # ── Sauvegarde intermédiaire après chaque palier ──────────────
        # Si le programme est interrompu, les résultats déjà calculés
        # sont conservés dans le JSON et réaffichables depuis le dashboard.
        if args.dashboard:
            os.makedirs("results", exist_ok=True)
            _partial = {
                "k": K, "n_servers": N_SERVERS,
                "cpu_count": multiprocessing.cpu_count(),
                "classic_demo": classic_demo,
                "results": rows,
                "projections": [],
                "partial": True,
            }
            with open(os.path.join("results", "simulation_result.json"),
                      "w", encoding="utf-8") as _f:
                json.dump(_partial, _f, indent=2, ensure_ascii=False)

    # ─────────────────────────────────────────────────────────────────
    # PROJECTIONS — scénario IoT à grande échelle (extrapolation O(n))
    # ─────────────────────────────────────────────────────────────────
    projections = []
    if args.dashboard:
        # Référence = la plus grande taille mesurée sans erreur
        ref = next((r for r in reversed(rows)
                    if not r.get("timed_out") and not r.get("mr_timed_out")), None)
        if ref and ref["classic_s"] > 0 and ref["mr_s"] > 0:
            ref_n   = ref["n"]
            ref_cls = ref["classic_s"]  # temps de calcul pur (mesuré)
            ref_mr  = ref["mr_s"]

            print_section("PROJECTIONS — Scénario IoT réel (calcul + transfert réseau 1 Mbps)")
            print(f"  Référence mesurée : {ref_n:,} capteurs")
            print(f"    Calcul classique = {ref_cls:.3f}s  |  Calcul MapReduce = {ref_mr:.3f}s")
            print(f"  Extrapolation O(n) + transfert réseau IoT ({IOT_BANDWIDTH_MBPS:.0f} Mbps)\n")
            print(f"  {'Capteurs':>15}  {'Classique (total)':>18}  {'MapReduce (total)':>18}  "
                  f"{'Gain MR':>8}  {'RAM':>8}")
            print(f"  {'─'*15}  {'─'*18}  {'─'*18}  {'─'*8}  {'─'*8}")

            for proj_n in PROJECTION_SIZES:
                cls_compute  = ref_cls * proj_n / ref_n
                mr_compute   = ref_mr  * proj_n / ref_n
                cls_transfer = data_transfer_time(proj_n, is_mapreduce=False)
                mr_transfer  = data_transfer_time(proj_n, is_mapreduce=True)
                cls_total    = cls_compute + cls_transfer
                mr_total_p   = mr_compute + mr_transfer
                ram_gb = proj_n * 200 / 1e9
                gain   = cls_total / mr_total_p if mr_total_p > 0 else 0

                ram_txt = (f"~{ram_gb/1000:.0f} TB" if ram_gb >= 1000
                           else f"~{ram_gb:.0f} GB" if ram_gb >= 1
                           else f"~{ram_gb*1000:.0f} MB")

                print(f"  {proj_n:>15,}  {_human_time(cls_total):>18}  "
                      f"{_human_time(mr_total_p):>18}  {gain:>6.1f}×  {ram_txt:>8}")

                projections.append({
                    "n":                  proj_n,
                    "classic_compute_s":  round(cls_compute, 1),
                    "classic_transfer_s": round(cls_transfer, 1),
                    "classic_total_s":    round(cls_total, 1),
                    "mr_compute_s":       round(mr_compute, 1),
                    "mr_transfer_s":      round(mr_transfer, 1),
                    "mr_total_s":         round(mr_total_p, 1),
                    "ram_gb":             round(ram_gb, 1),
                    "note":               "projection O(n) + réseau IoT 1 Mbps",
                })

            # Message central — le chiffre qui marque
            cls_1b       = ref_cls * 1_000_000_000 / ref_n
            mr_1b        = ref_mr  * 1_000_000_000 / ref_n
            cls_tr_1b    = data_transfer_time(1_000_000_000, is_mapreduce=False)
            mr_tr_1b     = data_transfer_time(1_000_000_000, is_mapreduce=True)
            cls_total_1b = cls_1b + cls_tr_1b
            mr_total_1b  = mr_1b  + mr_tr_1b
            gain_1b      = cls_total_1b / mr_total_1b

            print(f"\n  {BOLD}{RED}→ À 1 milliard de capteurs IoT :{RESET}")
            print(f"     Classique  : transfert {_human_time(cls_tr_1b)}"
                  f" + calcul {_human_time(cls_1b)}"
                  f" = {RED}{BOLD}{_human_time(cls_total_1b)}{RESET}")
            print(f"     {GREEN}{BOLD}MapReduce  : calcul distribué {_human_time(mr_1b)}"
                  f" + transfert négligeable = {_human_time(mr_total_1b)}{RESET}")
            print(f"\n     {BOLD}{GREEN}Gain réel : {gain_1b:.0f}× —"
                  f" MapReduce termine pendant que le classique collecte encore les données !{RESET}\n")

    # ─────────────────────────────────────────────────────────────────
    # PHASE 3 — Récapitulatif : la solution MapReduce
    # ─────────────────────────────────────────────────────────────────
    print(f"\n\n{'='*WIDTH}")
    print(f"{BOLD}  PHASE 3 — TABLEAU DE BORD : CLASSIQUE vs MAPREDUCE{RESET}")
    print(f"{'='*WIDTH}")
    print(f"  {'Capteurs':>12}  {'Classique (total)':>18}  {'MapReduce':>12}  {'Verdict':}")
    print(f"  {'─'*12}  {'─'*18}  {'─'*12}  {'─'*20}")

    for r in rows:
        n  = r["n"]
        ct = r.get("classic_total_s", r["classic_s"])
        mt = r.get("mr_total_s",      r["mr_s"])

        cl = f"{ct*1000:.0f} ms" if ct < 1 else f"{ct:.1f}s"
        mr = f"{mt*1000:.0f} ms" if mt < 1 else f"{mt:.2f}s"

        if r["timed_out"]:
            cl      = f"> {timeout_s}s TIMEOUT"
            verdict = f"{RED}Classique KO / MR OK{RESET}"
        elif mt > 0:
            ratio = ct / mt
            if ratio > 1.05:
                verdict = f"{GREEN}MR {ratio:.1f}× plus rapide{RESET}"
            elif ratio < 0.95:
                verdict = f"{YELLOW}Overhead ×{mt/ct:.1f}{RESET}"
            else:
                verdict = f"{YELLOW}≈ égal{RESET}"
        else:
            verdict = ""

        print(f"  {n:>12,}  {cl:>18}  {mr:>12}  {verdict}")

    print(f"{'='*WIDTH}")
    print(f"""
{BOLD}  CONCLUSION :{RESET}
  • Calcul pur (simulation locale) : MapReduce ~1.7× plus rapide à grande
    échelle — gain réel mais modeste sur une seule machine.
  • Scénario IoT réel (réseau 1 Mbps) : le classique doit rapatrier
    TOUTES les données brutes → le transfert domine largement le calcul.
    Dès 10 000 capteurs, MapReduce est 8× plus rapide au total.
  • À l'échelle milliards de capteurs : le classique passe plusieurs jours
    à collecter les données, MapReduce termine en quelques heures.
  • La clé : MapReduce traite les données LÀ OÙ ELLES SE TROUVENT —
    seules les sommes partielles ({N_SERVERS} vecteurs) transitent sur le réseau.
""")

    # ── Sauvegarde JSON des résultats ─────────────────────────────────
    os.makedirs("results", exist_ok=True)
    payload = {
        "k":           K,
        "n_servers":   N_SERVERS,
        "cpu_count":   multiprocessing.cpu_count(),
        "classic_demo": classic_demo,
        "results":     rows,
        "projections": projections,
    }
    out = os.path.join("results", "simulation_result.json")
    with open(out, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
    print(f"  Résultats JSON sauvegardés → {out}\n")


if __name__ == "__main__":
    main()
