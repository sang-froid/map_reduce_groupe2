# K-Means MapReduce — Capteurs IoT

**TP4 Big Data · Master 1 GL · Groupe 2 · IFRI**

---

## Problème

Des capteurs IoT répartis sur 5 serveurs régionaux (Nord, Centre, Sud, Ouest, Est) mesurent en continu température, humidité, vibration et trafic réseau. L'objectif est de regrouper ces capteurs en clusters homogènes selon leurs mesures, à l'aide de l'algorithme K-means implémenté en trois variantes : classique séquentiel, MapReduce parallèle et streaming continu.

---

## Structure du projet

```
map_reduce_groupe2/
├── main.py               # Point d'entrée unique — enchaîne toutes les étapes
├── dashboard.py          # Interface graphique Tkinter (dark theme)
├── generate_data.py      # Génère les données IoT simulées
├── kmeans_classic.py     # K-means séquentiel (K-means++, SSE, timeout)
├── kmeans_mr_job.py      # Mapper · Combiner · Shuffle · Reducer
├── kmeans_mapreduce.py   # Orchestrateur MapReduce (mode chunk et multi-serveurs)
├── kmeans_streaming.py   # K-means MapReduce en mode génération continue
├── benchmark.py          # Comparaison classique vs MapReduce (3 scénarios)
├── utils.py              # Fonctions partagées (euclidean, normalize, SSE, K-means++)
├── requirements.txt      # Aucune dépendance externe (stdlib uniquement)
├── data/
│   ├── server_nord.csv   # 300 capteurs — serveur régional Nord
│   ├── server_centre.csv # 300 capteurs — serveur régional Centre
│   ├── server_sud.csv    # 300 capteurs — serveur régional Sud
│   ├── server_ouest.csv  # 300 capteurs — serveur régional Ouest
│   ├── server_est.csv    # 300 capteurs — serveur régional Est
│   ├── all_sensors.csv   # Agrégé : 1 500 capteurs
│   └── large_sensors.csv # Stress test : 50 000 capteurs
└── results/              # Résultats JSON générés à l'exécution
    ├── classic_result.json
    ├── mapreduce_result.json
    ├── streaming_result.json
    └── benchmark.json
```

---

## Installation

Aucune dépendance externe. Python 3.8+ suffit — tout repose sur la bibliothèque standard (`multiprocessing`, `csv`, `json`, `tkinter`).

```bash
git clone <url-du-repo>
cd map_reduce_groupe2
```

---

## Exécution

### Tout-en-un (recommandé)

Lance dans l'ordre la génération de données, les 3 variantes K-means et le benchmark :

```bash
python main.py
```

### Interface graphique

Dashboard interactif avec terminal intégré, explorateur de données et visualisation des résultats :

```bash
python dashboard.py
```

Le dashboard permet de régler K, le nombre d'itérations, les paramètres de streaming, puis de lancer chaque algorithme d'un clic. Les résultats s'affichent en temps réel avec interprétation automatique des clusters.

---

### Exécution manuelle étape par étape

#### 1. Générer les données

```bash
python generate_data.py
```

#### 2. K-means classique — petit dataset

```bash
python kmeans_classic.py --input data/all_sensors.csv --k 3
```

#### 3. K-means classique — grand dataset avec timeout

```bash
python kmeans_classic.py --input data/large_sensors.csv --k 3 --timeout 30
```

#### 4. K-means MapReduce — mode chunk (un fichier découpé en partitions)

```bash
python kmeans_mapreduce.py data/all_sensors.csv --k 3 --max-iter 10
```

#### 5. K-means MapReduce — mode multi-serveurs (scénario IoT distribué)

```bash
python kmeans_mapreduce.py \
    --servers data/server_nord.csv data/server_centre.csv \
              data/server_sud.csv data/server_ouest.csv data/server_est.csv \
    --k 3 --max-iter 10
```

#### 6. Avec normalisation z-score (recommandé si les unités sont hétérogènes)

```bash
python kmeans_mapreduce.py data/all_sensors.csv --k 3 --normalize
```

#### 7. K-means streaming — génération continue de données

```bash
# 6 vagues de 100 mesures par serveur
python kmeans_streaming.py --waves 6 --sensors-per-wave 100 --k 3

# Avec délai entre vagues (simule la fréquence réelle des capteurs)
python kmeans_streaming.py --waves 10 --sensors-per-wave 50 --delay 2.0
```

#### 8. Benchmark comparatif complet

```bash
python benchmark.py
```

---

## Modélisation MapReduce

### Données distribuées

Chaque serveur régional traite ses propres capteurs en parallèle. Les résultats partiels sont ensuite agrégés par un reducer central.

```
Serveur Nord   ──► Mapper 1 ──► (cluster_id, somme_partielle, count)
Serveur Centre ──► Mapper 2 ──► (cluster_id, somme_partielle, count)
Serveur Sud    ──► Mapper 3 ──► (cluster_id, somme_partielle, count) ──► SHUFFLE ──► REDUCER
Serveur Ouest  ──► Mapper 4 ──► (cluster_id, somme_partielle, count)             (nouveau centroïde)
Serveur Est    ──► Mapper 5 ──► (cluster_id, somme_partielle, count)
```

### Phases

| Phase | Rôle |
|---|---|
| **Mapper** | Calcule la distance euclidienne de chaque capteur aux K centroïdes et l'assigne au cluster le plus proche |
| **Combiner** | Agrège localement les sommes partielles par cluster avant envoi (réduit le trafic réseau) |
| **Shuffle** | Regroupe toutes les sommes partielles par `cluster_id` |
| **Reducer** | Calcule le nouveau centroïde : somme globale / count total |

---

## Features utilisées pour le clustering

| Feature | Unité | Plage typique |
|---|---|---|
| temperature | °C | 13 – 38 |
| humidity | % | 30 – 90 |
| vibration | m/s² | 0.3 – 3.0 |
| network_traffic | kbps | 40 – 700 |

---

## Améliorations techniques

| Fonctionnalité | Description |
|---|---|
| **K-means++** | Initialisation intelligente des centroïdes — réduit les convergences vers de mauvais minima locaux |
| **SSE (inertie)** | Métrique de qualité des clusters affichée à chaque itération |
| **Normalisation z-score** | Empêche `network_traffic` (~500 kbps) d'écraser `temperature` (~30 °C) dans le calcul de distance |
| **Mode multi-serveurs** | Chaque serveur régional correspond à un worker MapReduce distinct |
| **Streaming warm-start** | Les centroïdes d'une vague servent d'initialisation à la vague suivante |
| **Timeout** | Le K-means classique peut être interrompu proprement sur les grands datasets |

---

## Résultats du benchmark

| Dataset | Points | Classique | MapReduce |
|---|---|---|---|
| Normal | 1 500 | ~0.15 s | ~0.30 s |
| Stress test | 50 000 | ~5.0 s | ~2.8 s |
| Multi-serveurs | 1 500 | ~0.15 s | ~0.30 s |

> Sur petit dataset, l'overhead de création des processus rend MapReduce légèrement plus lent.  
> Sur grand dataset (50 000 pts), MapReduce est ~1.8× plus rapide grâce au traitement parallèle.  
> En production réelle, où les données sont physiquement réparties sur le réseau, le gain est encore plus significatif car les mappers s'exécutent localement sur chaque serveur.
