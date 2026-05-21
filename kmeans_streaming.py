"""
kmeans_streaming.py
Simule la génération CONTINUE de données IoT et applique K-means MapReduce
en mode warm-start : les centroïdes de la vague précédente servent de point
de départ, évitant de repartir de zéro à chaque arrivée de nouvelles mesures.

Scénario :
    Toutes les `delay` secondes, chaque serveur régional reçoit
    `sensors_per_wave` nouvelles mesures de capteurs.
    Le système relance MapReduce (quelques itérations seulement) en partant
    des centroïdes courants. Les clusters se stabilisent progressivement.

Architecture :
    [Capteurs Nord]   → nouvelles mesures → buffer serveur Nord  ─┐
    [Capteurs Centre] → nouvelles mesures → buffer serveur Centre  │
    [Capteurs Sud]    → nouvelles mesures → buffer serveur Sud    ─┼→ MapReduce → centroïdes mis à jour
    [Capteurs Ouest]  → nouvelles mesures → buffer serveur Ouest   │
    [Capteurs Est]    → nouvelles mesures → buffer serveur Est    ─┘

Usage :
    python kmeans_streaming.py --waves 6 --sensors-per-wave 100 --k 3
    python kmeans_streaming.py --waves 10 --sensors-per-wave 50 --delay 1.0
"""

import random
import time
import json
import os
import argparse

from kmeans_mr_job import run_mapreduce_iteration
from utils import FEATURES, FEATURE_INDICES, euclidean, kmeans_plus_plus

# ─────────────────────────────────────────────
# Définition des serveurs régionaux
# ─────────────────────────────────────────────
REGIONS = {
    "server_nord":   {"lat_center": 12.5, "lon_center": 2.3},
    "server_centre": {"lat_center": 9.3,  "lon_center": 2.3},
    "server_sud":    {"lat_center": 6.4,  "lon_center": 2.4},
    "server_ouest":  {"lat_center": 9.5,  "lon_center": 1.2},
    "server_est":    {"lat_center": 9.5,  "lon_center": 3.5},
}

# 3 profils de comportement (temp, hum, vib, net) — génèrent 3 clusters naturels
BEHAVIOR_PROFILES = [
    (35.0, 2.0, 80.0, 5.0, 0.5, 0.1, 120.0, 20.0),   # chaud & humide (forêt)
    (22.0, 3.0, 45.0, 8.0, 2.5, 0.5, 500.0, 80.0),   # urbain actif
    (28.0, 2.5, 60.0, 6.0, 1.0, 0.2, 250.0, 40.0),   # suburbain modéré
]


# ─────────────────────────────────────────────
# Générateur de mesures
# ─────────────────────────────────────────────
def generate_wave_lines(region_name, n_sensors, sensor_id_start):
    """
    Simule l'arrivée de n_sensors nouvelles mesures sur un serveur régional.
    Retourne une liste de lignes CSV (format identique aux fichiers data/).
    """
    lines = []
    region = REGIONS[region_name]
    for i in range(n_sensors):
        profile = random.choice(BEHAVIOR_PROFILES)
        temp      = round(random.gauss(profile[0], profile[1]), 2)
        humidity  = round(max(0.0, min(100.0, random.gauss(profile[2], profile[3]))), 2)
        vibration = round(max(0.0, random.gauss(profile[4], profile[5])), 3)
        network   = round(max(0.0, random.gauss(profile[6], profile[7])), 2)
        lat = round(region["lat_center"] + random.gauss(0, 0.8), 4)
        lon = round(region["lon_center"] + random.gauss(0, 0.6), 4)
        sid = sensor_id_start + i
        lines.append(
            f"{sid},{region_name},{lat},{lon},"
            f"{temp},{humidity},{vibration},{network}\n"
        )
    return lines


def has_converged(old, new, tol=1e-4):
    return all(euclidean(o, n) < tol for o, n in zip(old, new))


# ─────────────────────────────────────────────
# K-means MapReduce en mode streaming
# ─────────────────────────────────────────────
def run_streaming_kmeans(k=3, n_waves=6, sensors_per_wave=100,
                         max_iter_per_wave=3, delay=0.0, seed=42):
    """
    Simule la génération continue de données IoT et applique K-means
    MapReduce en mode warm-start après chaque vague.

    Paramètres :
        k                 : nombre de clusters
        n_waves           : nombre de vagues à simuler
        sensors_per_wave  : nouvelles mesures par serveur par vague
        max_iter_per_wave : itérations MapReduce max par vague (warm-start)
        delay             : pause entre vagues en secondes (0 = instantané)
        seed              : graine aléatoire
    """
    random.seed(seed)
    region_names = list(REGIONS.keys())
    n_servers = len(region_names)

    # Un buffer de lignes CSV par serveur (accumule toutes les mesures reçues)
    server_buffers = {r: [] for r in region_names}
    sensor_counter = 0
    centroids = None
    history = []

    print(f"\n{'='*65}")
    print(f"  K-MEANS MAPREDUCE — Mode Streaming IoT")
    print(f"  K={k}  vagues={n_waves}  "
          f"capteurs/vague/serveur={sensors_per_wave}")
    print(f"  Itérations max par vague={max_iter_per_wave}  "
          f"serveurs={n_servers}")
    print(f"{'='*65}")

    for wave in range(1, n_waves + 1):
        wave_start = time.time()

        print(f"\n{'─'*65}")
        print(f"  VAGUE {wave}/{n_waves}  "
              f"(+{sensors_per_wave * n_servers} nouvelles mesures IoT)")
        print(f"{'─'*65}")

        # ── 1. Arrivée de nouvelles mesures sur chaque serveur ─────────
        for region in region_names:
            new_lines = generate_wave_lines(region, sensors_per_wave, sensor_counter)
            server_buffers[region].extend(new_lines)
            sensor_counter += sensors_per_wave

        total_pts = sum(len(buf) for buf in server_buffers.values())
        print(f"  Données accumulées : {total_pts} mesures  "
              f"(+{sensors_per_wave * n_servers} cette vague)\n")

        # ── 2. Initialisation K-means++ à la première vague ───────────
        if centroids is None:
            all_lines = [ln for buf in server_buffers.values() for ln in buf]
            points = []
            for line in all_lines:
                parts = line.strip().split(",")
                try:
                    points.append([float(parts[i]) for i in FEATURE_INDICES])
                except (ValueError, IndexError):
                    continue
            centroids = kmeans_plus_plus(points, k, seed=seed)
            print("  Initialisation K-means++ :")
            for i, c in enumerate(centroids):
                print(f"    C{i}: {dict(zip(FEATURES, [round(v, 2) for v in c]))}")
            print()

        # ── 3. MapReduce warm-start ───────────────────────────────────
        # Chaque serveur = un worker distinct (mode multi-serveurs)
        prev_centroids = [list(c) for c in centroids]
        server_lines = [server_buffers[r] for r in region_names]

        converged_at = max_iter_per_wave
        for it in range(1, max_iter_per_wave + 1):
            new_centroids, cluster_counts = run_mapreduce_iteration(
                server_lines, centroids, n_workers=n_servers
            )
            if has_converged(centroids, new_centroids):
                centroids = new_centroids
                converged_at = it
                break
            centroids = new_centroids

        print(f"  MapReduce : {converged_at} itération(s) "
              f"({'convergé' if converged_at < max_iter_per_wave else 'max atteint'})")

        # ── 4. Mesure de la dérive des centroïdes ─────────────────────
        # La dérive doit diminuer vague après vague → stabilisation
        drifts = [round(euclidean(prev_centroids[i], centroids[i]), 4)
                  for i in range(k)]

        # ── 5. Affichage des clusters ─────────────────────────────────
        print(f"\n  Clusters après vague {wave} :")
        for i, centroid in enumerate(centroids):
            vals = dict(zip(FEATURES, [round(v, 2) for v in centroid]))
            count = cluster_counts.get(i, 0)
            print(f"    Cluster {i} ({count:>5} capteurs) | dérive={drifts[i]:.4f}")
            print(f"      {vals}")

        wave_time = time.time() - wave_start
        print(f"\n  Durée vague {wave} : {wave_time:.3f}s")

        history.append({
            "wave": wave,
            "total_points": total_pts,
            "new_points": sensors_per_wave * n_servers,
            "centroids": [[round(v, 4) for v in c] for c in centroids],
            "cluster_counts": {str(cid): cnt for cid, cnt in cluster_counts.items()},
            "centroid_drifts": drifts,
            "converged_at_iter": converged_at,
            "duration_sec": round(wave_time, 4),
        })

        if delay > 0 and wave < n_waves:
            print(f"\n  Prochaine vague dans {delay}s...")
            time.sleep(delay)

    # ── Résumé final ───────────────────────────────────────────────────
    print(f"\n{'='*65}")
    print(f"  STREAMING TERMINÉ")
    print(f"  {n_waves} vagues  |  {sensor_counter} mesures totales  |  "
          f"{n_servers} serveurs")
    print(f"{'='*65}")
    _print_drift_summary(history, k)

    return centroids, cluster_counts, history


# ─────────────────────────────────────────────
# Résumé de la stabilisation
# ─────────────────────────────────────────────
def _print_drift_summary(history, k):
    """
    Affiche l'évolution de la dérive des centroïdes vague par vague.
    Une dérive décroissante confirme que les clusters se stabilisent.
    """
    header = f"  {'Vague':>5} {'Total pts':>10} " + \
             " ".join(f"{'C'+str(i)+' dérive':>12}" for i in range(k))
    print(f"\n  Évolution de la dérive des centroïdes :")
    print(header)
    print(f"  {'─'*5} {'─'*10} " + " ".join("─"*12 for _ in range(k)))
    for h in history:
        drifts_str = " ".join(f"{h['centroid_drifts'][i]:>12.4f}" for i in range(k))
        print(f"  {h['wave']:>5} {h['total_points']:>10} {drifts_str}")
    print()
    print("  Interprétation :")
    print("    → La dérive diminue à mesure que les données s'accumulent.")
    print("    → Les centroïdes se stabilisent sans repartir de zéro (warm-start).")
    print("    → En production, le délai entre vagues = fréquence réelle des capteurs.\n")


# ─────────────────────────────────────────────
# Point d'entrée
# ─────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="K-means MapReduce en mode streaming IoT"
    )
    parser.add_argument("--k",                 type=int,   default=3,
                        help="Nombre de clusters")
    parser.add_argument("--waves",             type=int,   default=6,
                        help="Nombre de vagues de données à simuler")
    parser.add_argument("--sensors-per-wave",  type=int,   default=100,
                        help="Nouvelles mesures par serveur par vague")
    parser.add_argument("--max-iter-per-wave", type=int,   default=3,
                        help="Itérations MapReduce max par vague (warm-start)")
    parser.add_argument("--delay",             type=float, default=0.0,
                        help="Délai en secondes entre les vagues (0 = instantané)")
    parser.add_argument("--output",            default="results/streaming_result.json")
    args = parser.parse_args()

    centroids, cluster_counts, history = run_streaming_kmeans(
        k=args.k,
        n_waves=args.waves,
        sensors_per_wave=args.sensors_per_wave,
        max_iter_per_wave=args.max_iter_per_wave,
        delay=args.delay,
    )

    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    result = {
        "algorithm": "kmeans_streaming_mapreduce",
        "k": args.k,
        "n_waves": args.waves,
        "sensors_per_wave_per_server": args.sensors_per_wave,
        "n_servers": len(REGIONS),
        "history": history,
        "final_centroids": [[round(v, 4) for v in c] for c in centroids],
        "final_cluster_counts": {str(cid): cnt for cid, cnt in cluster_counts.items()},
    }
    with open(args.output, "w") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)
    print(f"  Résultats sauvegardés → {args.output}\n")


if __name__ == "__main__":
    main()
