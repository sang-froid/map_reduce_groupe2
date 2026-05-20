# K-Means MapReduce — Capteurs IoT
**TP4 Big Data — IFRI Master 1 GL — Groupe 2**

## Problème
Regrouper des capteurs IoT répartis sur plusieurs serveurs régionaux en clusters
homogènes selon leurs mesures : température, humidité, vibration, trafic réseau.

## Structure du projet
```
kmeans_mapreduce/
├── generate_data.py        # Génère les données IoT simulées
├── kmeans_classic.py       # K-means séquentiel classique (avec timeout)
├── kmeans_mapreduce.py     # K-means distribué via mrjob
├── benchmark.py            # Comparaison classique vs MapReduce
├── requirements.txt        # Dépendances Python
├── data/                   # Générés par generate_data.py
│   ├── server_nord.csv
│   ├── server_centre.csv
│   ├── server_sud.csv
│   ├── server_ouest.csv
│   ├── server_est.csv
│   ├── all_sensors.csv     # Tous les capteurs (1 500 lignes)
│   └── large_sensors.csv  # Grand dataset (50 000 lignes)
└── results/                # Résultats JSON et rapports
```

## Installation
```bash
pip install mrjob
```

## Exécution

### 1. Générer les données
```bash
python generate_data.py
```

### 2. K-means classique (dataset normal)
```bash
python kmeans_classic.py --input data/all_sensors.csv --k 3
```

### 3. K-means classique avec timeout (grand dataset)
```bash
python kmeans_classic.py --input data/large_sensors.csv --k 3 --timeout 30
```

### 4. K-means MapReduce
```bash
python kmeans_mapreduce.py data/all_sensors.csv --k 3 --max-iter 10
```

### 5. Benchmark comparatif
```bash
python benchmark.py
```

## Modélisation MapReduce

### Mapper
- **Entrée** : une ligne CSV (un capteur) → vecteur de features
- **Traitement** : calcule la distance euclidienne aux K centroïdes courants
- **Sortie** : `(cluster_id → [features, count=1])`

### Combiner (optimisation)
- **Rôle** : somme partielle locale avant le shuffle réseau
- **Sortie** : `(cluster_id → [somme_partielle, count_partiel])`

### Reducer
- **Entrée** : tous les vecteurs d'un même cluster
- **Traitement** : calcule la moyenne → nouveau centroïde
- **Sortie** : `(cluster_id → nouveau_centroïde)`

## Features utilisées pour le clustering
| Feature           | Unité     |
|-------------------|-----------|
| temperature       | °C        |
| humidity          | %         |
| vibration         | m/s²      |
| network_traffic   | kbps      |
