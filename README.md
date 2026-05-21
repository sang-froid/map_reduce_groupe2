# K-Means MapReduce — Capteurs IoT
**TP4 Big Data — IFRI Master 1 GL — Groupe 2**

## Problème
Regrouper des capteurs IoT répartis sur plusieurs serveurs régionaux en clusters
homogènes selon leurs mesures : température, humidité, vibration, trafic réseau.

## Structure du projet
```
map_reduce_groupe2/
├── utils.py                # Fonctions partagées (euclidean, normalize, SSE, K-means++)
├── generate_data.py        # Génère les données IoT simulées (fichiers statiques)
├── kmeans_classic.py       # K-means séquentiel classique (avec timeout + SSE)
├── kmeans_mr_job.py        # Mapper, Combiner, Shuffle, Reducer (multiprocessing)
├── kmeans_mapreduce.py     # Orchestrateur : mode chunk et mode multi-serveurs
├── kmeans_streaming.py     # K-means MapReduce en mode génération continue (warm-start)
├── benchmark.py            # Comparaison classique vs MapReduce (3 scénarios)
├── requirements.txt        # Aucune dépendance externe (stdlib uniquement)
├── data/
│   ├── server_nord.csv     # 300 capteurs — serveur régional Nord
│   ├── server_centre.csv   # 300 capteurs — serveur régional Centre
│   ├── server_sud.csv      # 300 capteurs — serveur régional Sud
│   ├── server_ouest.csv    # 300 capteurs — serveur régional Ouest
│   ├── server_est.csv      # 300 capteurs — serveur régional Est
│   ├── all_sensors.csv     # Agrégé : 1 500 capteurs
│   └── large_sensors.csv   # Stress test : 50 000 capteurs
└── results/                # Résultats JSON
```

## Installation
Aucune dépendance externe. Python 3.8+ suffit.

## Exécution

### 1. Générer les données
```bash
python generate_data.py
```

### 2. K-means classique (petit dataset)
```bash
python kmeans_classic.py --input data/all_sensors.csv --k 3
```

### 3. K-means classique avec timeout (grand dataset)
```bash
python kmeans_classic.py --input data/large_sensors.csv --k 3 --timeout 30
```

### 4. K-means MapReduce — mode chunk (un fichier découpé)
```bash
python kmeans_mapreduce.py data/all_sensors.csv --k 3 --max-iter 10
```

### 5. K-means MapReduce — mode multi-serveurs (scénario IoT distribué)
```bash
python kmeans_mapreduce.py \
    --servers data/server_nord.csv data/server_centre.csv \
              data/server_sud.csv data/server_ouest.csv data/server_est.csv \
    --k 3 --max-iter 10
```

### 6. Avec normalisation z-score (recommandé)
```bash
python kmeans_mapreduce.py data/all_sensors.csv --k 3 --normalize
```

### 7. K-means MapReduce en mode streaming (génération continue)
```bash
# 6 vagues de 100 mesures par serveur (instantané)
python kmeans_streaming.py --waves 6 --sensors-per-wave 100 --k 3

# Avec délai entre vagues (simule la fréquence réelle des capteurs)
python kmeans_streaming.py --waves 10 --sensors-per-wave 50 --delay 2.0
```

### 8. Benchmark comparatif complet
```bash
python benchmark.py
```

## Modélisation MapReduce

### Données distribuées
Les données IoT sont stockées localement sur 5 serveurs régionaux (Nord, Centre, Sud,
Ouest, Est). Chaque serveur traite ses propres capteurs en parallèle.

```
Serveur Nord  ──► Mapper 1 ──► (cluster_id, somme_partielle, count)
Serveur Centre──► Mapper 2 ──► (cluster_id, somme_partielle, count)  ──► SHUFFLE ──► REDUCER
Serveur Sud   ──► Mapper 3 ──► (cluster_id, somme_partielle, count)                 (nouveau centroïde)
Serveur Ouest ──► Mapper 4 ──► ...
Serveur Est   ──► Mapper 5 ──► ...
```

### Mapper + Combiner
- **Entrée** : lignes CSV d'un serveur régional (300 capteurs)
- **Traitement MAP** : calcule la distance euclidienne aux K centroïdes → cluster le plus proche
- **Traitement COMBINE** : somme partielle locale par cluster (réduit le trafic réseau)
- **Sortie** : `{ cluster_id: (somme_partielle, count_partiel) }`

### Shuffle
- Regroupe tous les résultats partiels par `cluster_id`
- Sortie : `{ cluster_id: [(somme_1, count_1), (somme_2, count_2), ...] }`

### Reducer
- **Entrée** : toutes les sommes partielles d'un cluster
- **Traitement** : somme globale / count total → nouveau centroïde
- **Sortie** : `(cluster_id, nouveau_centroïde)`

## Améliorations techniques
| Fonctionnalité | Description |
|---|---|
| **K-means++** | Initialisation intelligente des centroïdes (réduction des mauvaises convergences) |
| **SSE (inertie)** | Métrique de qualité des clusters affiché à chaque itération |
| **Normalisation z-score** | Évite que `network_traffic` (~500 kbps) domine `temperature` (~30 °C) |
| **Mode multi-serveurs** | Chaque serveur régional = un worker MapReduce distinct |
| **Module `utils.py`** | Fonctions partagées sans duplication de code |

## Features utilisées pour le clustering
| Feature           | Unité     | Plage typique |
|-------------------|-----------|---------------|
| temperature       | °C        | 13 – 38       |
| humidity          | %         | 30 – 90       |
| vibration         | m/s²      | 0.3 – 3.0     |
| network_traffic   | kbps      | 40 – 700      |

## Résultats du benchmark
| Dataset               | Points | Classique | MapReduce |
|-----------------------|--------|-----------|-----------|
| Normal                |  1 500 |  ~0.15s   |  ~0.30s   |
| Stress test           | 50 000 |  ~5.0s    |  ~2.8s    |
| Multi-serveurs        |  1 500 |  ~0.15s   |  ~0.30s   |

> Sur petit dataset, l'overhead de création des processus domine.
> Sur grand dataset (50 000 pts), MapReduce est ~1.8x plus rapide.
> En production réelle (données réparties sur le réseau), le gain est bien plus important.
