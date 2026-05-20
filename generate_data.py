"""
generate_data.py
Génère des données IoT simulées réparties sur plusieurs serveurs régionaux.
Chaque serveur produit un fichier CSV représentant ses capteurs locaux.

Colonnes : sensor_id, region, latitude, longitude, temperature, humidity, vibration, network_traffic
"""

import random
import math
import os
import json

# Reproductibilité
random.seed(42)

# Définition des régions (serveurs) avec leurs centroïdes géographiques approximatifs
REGIONS = {
    "server_nord":    {"lat_center": 12.5,  "lon_center": 2.3,   "n_sensors": 300},
    "server_centre":  {"lat_center": 9.3,   "lon_center": 2.3,   "n_sensors": 300},
    "server_sud":     {"lat_center": 6.4,   "lon_center": 2.4,   "n_sensors": 300},
    "server_ouest":   {"lat_center": 9.5,   "lon_center": 1.2,   "n_sensors": 300},
    "server_est":     {"lat_center": 9.5,   "lon_center": 3.5,   "n_sensors": 300},
}

# Profils de capteurs : 3 clusters naturels de comportement
BEHAVIOR_PROFILES = [
    # (temp_mean, temp_std, hum_mean, hum_std, vib_mean, vib_std, net_mean, net_std)
    (35.0, 2.0,  80.0, 5.0,  0.5, 0.1,  120.0, 20.0),  # chaud & humide (ex: forêt)
    (22.0, 3.0,  45.0, 8.0,  2.5, 0.5,  500.0, 80.0),  # urbain actif
    (28.0, 2.5,  60.0, 6.0,  1.0, 0.2,  250.0, 40.0),  # suburbain modéré
]

def gauss(mean, std):
    return random.gauss(mean, std)

def generate_sensor(sensor_id, region_name, region_info):
    lat = gauss(region_info["lat_center"], 0.8)
    lon = gauss(region_info["lon_center"], 0.6)
    profile = random.choice(BEHAVIOR_PROFILES)
    temp     = round(gauss(profile[0], profile[1]), 2)
    humidity = round(max(0, min(100, gauss(profile[2], profile[3]))), 2)
    vibration= round(max(0, gauss(profile[4], profile[5])), 3)
    network  = round(max(0, gauss(profile[6], profile[7])), 2)
    return {
        "sensor_id":       sensor_id,
        "region":          region_name,
        "latitude":        round(lat, 4),
        "longitude":       round(lon, 4),
        "temperature":     temp,
        "humidity":        humidity,
        "vibration":       vibration,
        "network_traffic": network,
    }

def generate_all(output_dir="data"):
    os.makedirs(output_dir, exist_ok=True)
    all_sensors = []
    sensor_counter = 0

    for region_name, region_info in REGIONS.items():
        filepath = os.path.join(output_dir, f"{region_name}.csv")
        with open(filepath, "w") as f:
            f.write("sensor_id,region,latitude,longitude,temperature,humidity,vibration,network_traffic\n")
            for _ in range(region_info["n_sensors"]):
                s = generate_sensor(sensor_counter, region_name, region_info)
                f.write(f"{s['sensor_id']},{s['region']},{s['latitude']},{s['longitude']},"
                        f"{s['temperature']},{s['humidity']},{s['vibration']},{s['network_traffic']}\n")
                all_sensors.append(s)
                sensor_counter += 1
        print(f"[OK] {filepath} — {region_info['n_sensors']} capteurs")

    # Aussi un fichier global (pour le classique et le benchmark)
    global_path = os.path.join(output_dir, "all_sensors.csv")
    with open(global_path, "w") as f:
        f.write("sensor_id,region,latitude,longitude,temperature,humidity,vibration,network_traffic\n")
        for s in all_sensors:
            f.write(f"{s['sensor_id']},{s['region']},{s['latitude']},{s['longitude']},"
                    f"{s['temperature']},{s['humidity']},{s['vibration']},{s['network_traffic']}\n")
    print(f"[OK] {global_path} — {sensor_counter} capteurs au total")

    # Génération d'un grand dataset pour le benchmark (stress test)
    large_path = os.path.join(output_dir, "large_sensors.csv")
    with open(large_path, "w") as f:
        f.write("sensor_id,region,latitude,longitude,temperature,humidity,vibration,network_traffic\n")
        for i in range(50000):
            region_name = random.choice(list(REGIONS.keys()))
            region_info = REGIONS[region_name]
            s = generate_sensor(sensor_counter + i, region_name, region_info)
            f.write(f"{s['sensor_id']},{s['region']},{s['latitude']},{s['longitude']},"
                    f"{s['temperature']},{s['humidity']},{s['vibration']},{s['network_traffic']}\n")
    print(f"[OK] {large_path} — 50 000 capteurs (benchmark)")

    return sensor_counter

if __name__ == "__main__":
    total = generate_all()
    print(f"\nTotal capteurs générés (données normales) : {total}")
    print("Fichiers disponibles dans le dossier data/")
