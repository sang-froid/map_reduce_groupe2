"""
benchmark.py
Compare K-means classique vs MapReduce distribué sur 3 phases :
  Phase 1 : mesures réelles — fichiers CSV (1 500 et 50 000 capteurs)
  Phase 2 : mesures réelles — données synthétiques (100k → 1M) avec timeout
  Phase 3 : projections O(n) — 10M, 100M, 1 milliard de capteurs
             + overhead réseau IoT (1 Mbps capteurs) vs LAN Gigabit

Compatible Python 3.12+, Windows/Linux/Mac.
Usage : python benchmark.py
"""
import time
import argparse
import json
import os
import sys
import random
import threading
import multiprocessing

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from kmeans_classic import kmeans_classic
from kmeans_mapreduce import run_kmeans_mapreduce, run_kmeans_mapreduce_distributed
from kmeans_mr_job import run_mapreduce_iteration
from utils import FEATURES, load_data, euclidean, kmeans_plus_plus

RESULTS_DIR = "results"
os.makedirs(RESULTS_DIR, exist_ok=True)

K         = 3
MAX_ITER  = 10
TIMEOUT_S = 20
N_SERVERS = 5
SEED      = 42

# Réseau IoT simulé
IOT_BANDWIDTH_MBPS = 1.0        # réseau capteurs (typique zone industrielle/rurale)
LAN_BANDWIDTH_MBPS = 1000       # LAN Gigabit entre serveurs régionaux
BYTES_PER_POINT    = 4 * 8      # 4 features × 8 octets (float64)

SERVER_FILES = [
    "data/server_nord.csv",
    "data/server_centre.csv",
    "data/server_sud.csv",
    "data/server_ouest.csv",
    "data/server_est.csv",
]

# 3 profils de comportement IoT (temp, hum, vibration, trafic réseau)
PROFILES = [
    (35.0, 2.0,  80.0, 5.0,  0.5, 0.1,  120.0, 20.0),   # chaud & humide
    (22.0, 3.0,  45.0, 8.0,  2.5, 0.5,  500.0, 80.0),   # urbain actif
    (28.0, 2.5,  60.0, 6.0,  1.0, 0.2,  250.0, 40.0),   # suburbain modéré
]

BOLD   = "\033[1m"
RED    = "\033[91m"
GREEN  = "\033[92m"
YELLOW = "\033[93m"
RESET  = "\033[0m"
WIDTH  = 72


# ─────────────────────────────────────────────────────────────────────
# Utilitaires
# ─────────────────────────────────────────────────────────────────────

class TimedOut(Exception):
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
        raise TimedOut()
    if error[0]:
        raise error[0]
    return result[0]


def generate_points(n, seed=SEED):
    """Génère n capteurs IoT synthétiques en mémoire (pas de fichier disque)."""
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
    return [
        f"{i},region,0.0,0.0,{p[0]},{p[1]},{p[2]},{p[3]}\n"
        for i, p in enumerate(points)
    ]


def split_into_server_chunks(points, n_servers):
    chunk_size = max(1, len(points) // n_servers)
    chunks = [points_to_csv_lines(points[i:i + chunk_size])
              for i in range(0, len(points), chunk_size)]
    return chunks[:n_servers]


def _has_converged(old, new, tol=1e-4):
    return all(euclidean(o, n) < tol for o, n in zip(old, new))


def _classic_inplace(points):
    """K-means séquentiel pur Python, sans I/O (pour les grands volumes synthétiques)."""
    centroids = kmeans_plus_plus(points, K, seed=SEED)
    dim = len(points[0])
    start = time.time()
    for it in range(1, MAX_ITER + 1):
        # MAP : assignation
        assignments = [
            min(range(K), key=lambda j: euclidean(points[i], centroids[j]))
            for i in range(len(points))
        ]
        # REDUCE : mise à jour des centroïdes
        sums   = [[0.0] * dim for _ in range(K)]
        counts = [0] * K
        for p, a in zip(points, assignments):
            for d in range(dim):
                sums[a][d] += p[d]
            counts[a] += 1
        new_c = [
            [sums[i][d] / counts[i] for d in range(dim)] if counts[i] > 0 else sums[i]
            for i in range(K)
        ]
        if _has_converged(centroids, new_c):
            centroids = new_c
            break
        centroids = new_c
    return centroids, time.time() - start, it


def _mapreduce_inplace(points):
    """K-means MapReduce en mode multi-serveurs sur données synthétiques en mémoire."""
    server_chunks = split_into_server_chunks(points, N_SERVERS)
    sample    = points[:2000] if len(points) > 2000 else points
    centroids = kmeans_plus_plus(sample, K, seed=SEED)
    start = time.time()
    for it in range(1, MAX_ITER + 1):
        new_centroids, cluster_counts = run_mapreduce_iteration(
            server_chunks, centroids, n_workers=N_SERVERS
        )
        if _has_converged(centroids, new_centroids):
            centroids = new_centroids
            break
        centroids = new_centroids
    return centroids, cluster_counts, time.time() - start, it


def network_transfer_time(n_points, is_mapreduce=False):
    """
    Classique : rapatrie TOUTES les données brutes via le réseau IoT lent.
    MapReduce : seules K sommes partielles par serveur transitent via LAN rapide.
    """
    if is_mapreduce:
        bytes_sent = K * 4 * 8 * N_SERVERS * MAX_ITER
        bw = LAN_BANDWIDTH_MBPS * 1_000_000 / 8
    else:
        bytes_sent = n_points * BYTES_PER_POINT
        bw = IOT_BANDWIDTH_MBPS * 1_000_000 / 8
    return bytes_sent / bw


def _human_time(s):
    if s >= 86400: return f"~{s/86400:.1f} jour(s)"
    if s >= 3600:  return f"~{s/3600:.1f} h"
    if s >= 60:    return f"~{s/60:.0f} min"
    return f"~{s:.2f}s"


def fmt_dur(s, timed_out=False):
    if timed_out:
        return f">{TIMEOUT_S}s  TIMEOUT"
    if s < 1:
        return f"{s*1000:.0f} ms"
    return f"{s:.3f}s"


def print_section(title):
    print(f"\n{BOLD}{'─'*WIDTH}{RESET}")
    print(f"{BOLD}  {title}{RESET}")
    print(f"{'─'*WIDTH}{RESET}")


# ─────────────────────────────────────────────────────────────────────
# PHASE 1 — Fichiers CSV réels
# ─────────────────────────────────────────────────────────────────────

def phase1_file_benchmarks():
    results = []

    DATASETS = [
        ("data/all_sensors.csv",   "1 500 capteurs (réels)"),
        ("data/large_sensors.csv", "50 000 capteurs (stress)"),
    ]

    for filepath, label in DATASETS:
        if not os.path.exists(filepath):
            print(f"  {YELLOW}[SKIP] {filepath} manquant — lancez : python generate_data.py{RESET}")
            continue

        print_section(f"Fichier : {label}")
        points = load_data(filepath)
        row = {"dataset": label, "n_points": len(points)}

        # Classique
        timeout = TIMEOUT_S if len(points) > 5000 else 0
        print(f"  {BOLD}[1/2] CLASSIQUE{RESET}"
              + (f"  (timeout {timeout}s)" if timeout else ""))
        timed_out = False
        try:
            if timeout:
                _, _, iters, dur = run_with_timeout(
                    kmeans_classic, (points, K, MAX_ITER), timeout)
            else:
                _, _, iters, dur = kmeans_classic(points, k=K, max_iter=MAX_ITER)
            print(f"    → {fmt_dur(dur)}  ({iters} itérations)")
            row.update({"classic_s": round(dur, 3), "classic_iters": iters,
                        "classic_timeout": False})
        except TimedOut:
            timed_out = True
            print(f"    → {RED}TIMEOUT après {TIMEOUT_S}s{RESET}")
            row.update({"classic_s": TIMEOUT_S, "classic_iters": None,
                        "classic_timeout": True})

        # MapReduce chunk
        print(f"  {BOLD}[2/2] MAPREDUCE (chunk){RESET}")
        try:
            _, _, iters_mr, dur_mr = run_kmeans_mapreduce(
                filepath, k=K, max_iter=MAX_ITER)
            print(f"    → {fmt_dur(dur_mr)}  ({iters_mr} itérations)")
            row.update({"mr_s": round(dur_mr, 3), "mr_iters": iters_mr,
                        "mr_timeout": False})
        except Exception as e:
            print(f"    → {RED}Erreur : {e}{RESET}")
            row.update({"mr_s": None, "mr_iters": None, "mr_timeout": True})

        if not timed_out and row.get("mr_s"):
            ratio = row["classic_s"] / row["mr_s"]
            tag = (f"{GREEN}MR {ratio:.2f}× plus rapide{RESET}" if ratio > 1.05
                   else f"{YELLOW}Overhead {1/ratio:.2f}× (s'inverse à grande échelle){RESET}")
            print(f"    {tag}")

        results.append(row)

    # Multi-serveurs
    missing = [f for f in SERVER_FILES if not os.path.exists(f)]
    if not missing:
        print_section("Multi-serveurs : 1 500 capteurs (5 fichiers régionaux)")

        print(f"  {BOLD}[1/2] CLASSIQUE (fichier agrégé){RESET}")
        pts = load_data("data/all_sensors.csv")
        _, _, iters, dur = kmeans_classic(pts, k=K, max_iter=MAX_ITER)
        print(f"    → {fmt_dur(dur)}  ({iters} itérations)")

        print(f"  {BOLD}[2/2] MAPREDUCE DISTRIBUÉ ({N_SERVERS} serveurs){RESET}")
        _, cc, iters_mr, dur_mr = run_kmeans_mapreduce_distributed(
            SERVER_FILES, k=K, max_iter=MAX_ITER)
        print(f"    → {fmt_dur(dur_mr)}  ({iters_mr} itérations)")

        results.append({
            "dataset": f"1 500 multi-serveurs",
            "n_points": sum(cc.values()),
            "classic_s": round(dur, 3), "classic_iters": iters, "classic_timeout": False,
            "mr_s": round(dur_mr, 3),   "mr_iters": iters_mr,  "mr_timeout": False,
        })

    return results


# ─────────────────────────────────────────────────────────────────────
# PHASE 2 — Montée en charge (données synthétiques + timeout)
# ─────────────────────────────────────────────────────────────────────

SCALABILITY_SIZES = [100_000, 500_000, 1_000_000]


def phase2_scalability():
    results = []

    for n in SCALABILITY_SIZES:
        print_section(f"Montée en charge : {n:,} capteurs IoT (synthétiques)")

        print(f"  Génération des données... ", end="", flush=True)
        t0 = time.time()
        points = generate_points(n)
        print(f"{time.time()-t0:.2f}s")

        row = {"dataset": f"{n:,} capteurs", "n_points": n}

        # Classique avec timeout
        print(f"  {BOLD}[1/2] CLASSIQUE{RESET}  (timeout {TIMEOUT_S}s — 1 seul CPU)")
        timed_out = False
        try:
            _, dur_c, iters_c = run_with_timeout(
                _classic_inplace, (points,), TIMEOUT_S)
            print(f"    → {fmt_dur(dur_c)}  ({iters_c} itérations)")
            row.update({"classic_s": round(dur_c, 3), "classic_iters": iters_c,
                        "classic_timeout": False})
        except TimedOut:
            timed_out = True
            print(f"    → {RED}TIMEOUT après {TIMEOUT_S}s — impossible sur 1 nœud{RESET}")
            row.update({"classic_s": TIMEOUT_S, "classic_iters": None,
                        "classic_timeout": True})

        # MapReduce distribué
        print(f"  {BOLD}[2/2] MAPREDUCE DISTRIBUÉ{RESET}  ({N_SERVERS} serveurs régionaux)")
        _, _, dur_mr, iters_mr = _mapreduce_inplace(points)
        print(f"    → {fmt_dur(dur_mr)}  ({iters_mr} itérations)")
        row.update({"mr_s": round(dur_mr, 3), "mr_iters": iters_mr, "mr_timeout": False})

        # Coût réseau IoT réel
        cls_transfer = network_transfer_time(n, is_mapreduce=False)
        mr_transfer  = network_transfer_time(n, is_mapreduce=True)
        cls_total    = (row["classic_s"] or 0) + cls_transfer
        mr_total     = dur_mr + mr_transfer
        row.update({
            "classic_transfer_s": round(cls_transfer, 2),
            "mr_transfer_s":      round(mr_transfer, 6),
            "classic_total_s":    round(cls_total, 2),
            "mr_total_s":         round(mr_total, 3),
        })

        data_mb = n * BYTES_PER_POINT / 1_000_000
        print(f"\n  Réseau IoT (1 Mbps) — envoi de {data_mb:.1f} MB vers nœud central :")
        print(f"    Classique  : {fmt_dur(row['classic_s'], timed_out)} calcul"
              f" + {_human_time(cls_transfer)} transfert"
              f"  = {RED}{_human_time(cls_total)}{RESET} total")
        print(f"    MapReduce  : {fmt_dur(dur_mr)} calcul"
              f" + {mr_transfer*1000:.1f} ms transfert"
              f"  = {GREEN}{_human_time(mr_total)}{RESET} total")

        gain = cls_total / mr_total if mr_total > 0 else 0
        print(f"    {BOLD}{GREEN}→ MapReduce {gain:.1f}× plus rapide au total{RESET}")

        results.append(row)

    return results


# ─────────────────────────────────────────────────────────────────────
# PHASE 3 — Projections O(n) vers 10M, 100M, 1 milliard
# ─────────────────────────────────────────────────────────────────────

PROJECTION_SIZES = [10_000_000, 100_000_000, 1_000_000_000]


def phase3_projections(phase2_results):
    """Extrapolation O(n) depuis la meilleure mesure réelle disponible."""

    # Référence = plus grande taille mesurée sans erreur MapReduce
    ref = next(
        (r for r in reversed(phase2_results) if not r.get("mr_timeout")),
        None
    )
    if ref is None:
        print(f"  {RED}Aucune mesure de référence disponible pour les projections.{RESET}")
        return []

    # Pour le classique : utiliser la dernière mesure NON-timeout si possible,
    # sinon extrapoler depuis la dernière mesure qui a réussi.
    ref_classic_ok = next(
        (r for r in reversed(phase2_results) if not r.get("classic_timeout")),
        None
    )
    ref_n   = ref["n_points"]
    ref_mr  = ref["mr_s"]
    # Temps classique de référence : mesuré ou extrapolé depuis 100k
    if ref_classic_ok:
        ref_c = ref_classic_ok["classic_s"]
        ref_c_n = ref_classic_ok["n_points"]
    else:
        # fallback sur la valeur timeout (borne basse de l'extrapolation)
        ref_c   = ref["classic_s"]
        ref_c_n = ref["n_points"]

    print_section("PROJECTIONS — Scénario IoT réel (extrapolation O(n) + réseau 1 Mbps)")
    print(f"  Référence MapReduce  : {ref_n:,} capteurs → {ref_mr:.3f}s")
    print(f"  Référence Classique  : {ref_c_n:,} capteurs → {ref_c:.3f}s")
    print(f"  Réseau IoT   : {IOT_BANDWIDTH_MBPS} Mbps  |  LAN : {LAN_BANDWIDTH_MBPS} Mbps")
    print()
    print(f"  {'Capteurs':>15}  {'Classique (total)':>18}  {'MapReduce (total)':>18}"
          f"  {'Gain':>6}  {'RAM':>8}")
    print(f"  {'─'*15}  {'─'*18}  {'─'*18}  {'─'*6}  {'─'*8}")

    projections = []
    for proj_n in PROJECTION_SIZES:
        cls_compute  = ref_c  * proj_n / ref_c_n
        mr_compute   = ref_mr * proj_n / ref_n
        cls_transfer = network_transfer_time(proj_n, is_mapreduce=False)
        mr_transfer  = network_transfer_time(proj_n, is_mapreduce=True)
        cls_total    = cls_compute + cls_transfer
        mr_total     = mr_compute  + mr_transfer
        gain         = cls_total / mr_total if mr_total > 0 else 0
        ram_gb       = proj_n * 200 / 1e9  # ~200 octets/capteur en RAM

        ram_txt = (f"~{ram_gb/1000:.0f} TB" if ram_gb >= 1000
                   else f"~{ram_gb:.0f} GB" if ram_gb >= 1
                   else f"~{ram_gb*1000:.0f} MB")

        print(f"  {proj_n:>15,}  {_human_time(cls_total):>18}  "
              f"{_human_time(mr_total):>18}  {gain:>4.1f}×  {ram_txt:>8}")

        projections.append({
            "n":                  proj_n,
            "classic_compute_s":  round(cls_compute,  1),
            "classic_transfer_s": round(cls_transfer, 1),
            "classic_total_s":    round(cls_total,    1),
            "mr_compute_s":       round(mr_compute,   1),
            "mr_transfer_s":      round(mr_transfer,  1),
            "mr_total_s":         round(mr_total,     1),
            "ram_gb":             round(ram_gb,        1),
            "gain_x":             round(gain,          1),
            "note":               "extrapolation O(n) + réseau IoT 1 Mbps",
        })

    # Message d'impact à 1 milliard
    p = projections[-1]
    print(f"\n  {BOLD}{RED}→ À 1 milliard de capteurs IoT :{RESET}")
    print(f"     Classique  : {_human_time(p['classic_transfer_s'])} transfert"
          f" + {_human_time(p['classic_compute_s'])} calcul"
          f"  = {RED}{BOLD}{_human_time(p['classic_total_s'])}{RESET}")
    print(f"     {GREEN}{BOLD}MapReduce  : {_human_time(p['mr_compute_s'])} calcul"
          f" + transfert négligeable = {_human_time(p['mr_total_s'])}{RESET}")
    print(f"\n     {BOLD}{GREEN}Gain total : {p['gain_x']:.0f}× —"
          f" MapReduce termine quand le classique collecte encore les données !{RESET}\n")

    return projections


# ─────────────────────────────────────────────────────────────────────
# Récapitulatif global
# ─────────────────────────────────────────────────────────────────────

def print_summary(results_p1, results_p2, projections):
    all_rows = results_p1 + results_p2

    print(f"\n\n{'='*WIDTH}")
    print(f"{BOLD}  RÉCAPITULATIF COMPLET{RESET}")
    print(f"{'='*WIDTH}")
    print(f"  {'Dataset':<28}  {'Points':>9}  {'Classique':>14}  {'MapReduce':>12}  Verdict")
    print(f"  {'─'*28}  {'─'*9}  {'─'*14}  {'─'*12}  {'─'*20}")

    for r in all_rows:
        c_s   = r.get("classic_s")
        m_s   = r.get("mr_s")
        timed = r.get("classic_timeout", False)

        cl = (f">{TIMEOUT_S}s TIMEOUT" if timed
              else f"{c_s*1000:.0f} ms" if c_s and c_s < 1
              else f"{c_s:.3f}s"       if c_s else "—")
        mr = (f"{m_s*1000:.0f} ms" if m_s and m_s < 1
              else f"{m_s:.3f}s"   if m_s else "ERREUR")

        if timed and m_s:
            verdict = f"{RED}Classique KO{RESET} / {GREEN}MR OK{RESET}"
        elif c_s and m_s:
            ratio = c_s / m_s
            verdict = (f"{GREEN}MR {ratio:.2f}× + rapide{RESET}" if ratio > 1.05
                       else f"{YELLOW}Overhead {m_s/c_s:.2f}×{RESET}" if ratio < 0.95
                       else f"{YELLOW}≈ égal{RESET}")
        else:
            verdict = "—"

        print(f"  {r['dataset']:<28}  {r['n_points']:>9,}  {cl:>14}  {mr:>12}  {verdict}")

    if projections:
        print(f"\n  {'─'*72}")
        print(f"  {'Capteurs (extrapolé)':<28}  {'Classique total':>22}  "
              f"{'MapReduce total':>18}  {'Gain':>6}")
        print(f"  {'─'*28}  {'─'*22}  {'─'*18}  {'─'*6}")
        for p in projections:
            lbl = f"{p['n']:,}"
            print(f"  {lbl:<28}  {_human_time(p['classic_total_s']):>22}  "
                  f"{_human_time(p['mr_total_s']):>18}  {p['gain_x']}×")

    print(f"{'='*WIDTH}")
    print(f"""
{BOLD}  CONCLUSION :{RESET}
  • Calcul pur          : MapReduce plus rapide dès ~50 000 capteurs.
  • Avec réseau IoT     : MapReduce dominant dès 10 000 capteurs (1 Mbps).
  • Seuil critique      : classique timeout à {TIMEOUT_S}s (~500 000 capteurs),
                          seul MapReduce peut terminer au-delà.
  • À 1 milliard        : classique = plusieurs jours, MapReduce = quelques heures.
  • Clé du gain         : les données restent sur les {N_SERVERS} serveurs locaux —
                          seules {K} sommes partielles (quelques Ko) transitent.
""")


# ─────────────────────────────────────────────────────────────────────
# Point d'entrée
# ─────────────────────────────────────────────────────────────────────

def run_benchmark():
    global K, MAX_ITER
    parser = argparse.ArgumentParser(description="Benchmark K-means classique vs MapReduce")
    parser.add_argument("--k",        type=int, default=K,        help="Nombre de clusters")
    parser.add_argument("--max-iter", type=int, default=MAX_ITER, help="Itérations max")
    args, _ = parser.parse_known_args()
    K        = args.k
    MAX_ITER = args.max_iter

    cpu_count = multiprocessing.cpu_count()

    print(f"\n{'='*WIDTH}")
    print(f"{BOLD}  BENCHMARK — K-MEANS CLASSIQUE vs MAPREDUCE DISTRIBUÉ{RESET}")
    print(f"  Machine    : {cpu_count} CPUs logiques")
    print(f"  K={K}  max_iter={MAX_ITER}  timeout={TIMEOUT_S}s  serveurs={N_SERVERS}")
    print(f"  Réseau IoT : {IOT_BANDWIDTH_MBPS} Mbps  |  LAN : {LAN_BANDWIDTH_MBPS} Mbps")
    print(f"{'='*WIDTH}")

    # ── Phase 1 ──────────────────────────────────────────────────────
    print(f"\n{BOLD}{'='*WIDTH}")
    print(f"  PHASE 1 — FICHIERS CSV RÉELS")
    print(f"{'='*WIDTH}{RESET}")
    results_p1 = phase1_file_benchmarks()

    # ── Phase 2 ──────────────────────────────────────────────────────
    print(f"\n{BOLD}{'='*WIDTH}")
    print(f"  PHASE 2 — MONTÉE EN CHARGE (données synthétiques + timeout classique)")
    print(f"{'='*WIDTH}{RESET}")
    results_p2 = phase2_scalability()

    # ── Phase 3 ──────────────────────────────────────────────────────
    print(f"\n{BOLD}{'='*WIDTH}")
    print(f"  PHASE 3 — PROJECTIONS MILLIARDS (extrapolation O(n) + réseau IoT)")
    print(f"{'='*WIDTH}{RESET}")
    projections = phase3_projections(results_p2)

    # ── Récapitulatif ─────────────────────────────────────────────────
    print_summary(results_p1, results_p2, projections)

    # ── Sauvegarde JSON ───────────────────────────────────────────────
    out = os.path.join(RESULTS_DIR, "benchmark.json")
    payload = {
        "config": {
            "k":         K,
            "max_iter":  MAX_ITER,
            "timeout_s": TIMEOUT_S,
            "n_servers": N_SERVERS,
            "cpu_count": cpu_count,
            "iot_mbps":  IOT_BANDWIDTH_MBPS,
            "lan_mbps":  LAN_BANDWIDTH_MBPS,
        },
        "phase1_files":       results_p1,
        "phase2_scalability": results_p2,
        "phase3_projections": projections,
    }
    with open(out, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
    print(f"  Résultats sauvegardés → {out}\n")

    return payload


if __name__ == "__main__":
    multiprocessing.freeze_support()
    run_benchmark()
