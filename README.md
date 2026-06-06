# K-Means MapReduce — Clustering de capteurs IoT

**TP4 Big Data · Master 1 GL · Groupe 2 · IFRI 2025-2026**

---

## Contexte

On dispose de 5 serveurs régionaux (Nord, Centre, Sud, Ouest, Est), chacun collectant en continu les mesures de 300 capteurs IoT (température, humidité, vibration, trafic réseau). L'objectif est de regrouper ces capteurs en zones homogènes via K-Means, en comparant trois approches : séquentielle classique, MapReduce parallèle, et streaming continu.

---

## Structure du projet

```
map_reduce_groupe2/
├── main.py                  # Lance tout dans l'ordre (génération → benchmark)
├── dashboard.py             # Interface Tkinter — dark theme
├── generate_data.py         # Génère les données simulées des 5 serveurs
├── kmeans_classic.py        # K-Means séquentiel avec K-Means++ et timeout
├── kmeans_mr_job.py         # Le cœur : Mapper, Combiner, Shuffle, Reducer
├── kmeans_mapreduce.py      # Orchestrateur — mode chunk ou multi-serveurs
├── kmeans_streaming.py      # Mode warm-start : vagues successives de capteurs
├── benchmark.py             # Compare classique vs MapReduce sur 3 datasets
├── simulation_comparison.py # Montée en charge jusqu'à 1M de capteurs
├── utils.py                 # Fonctions communes : distance, SSE, K-Means++, z-score
├── requirements.txt
├── data/
│   ├── server_nord.csv      # 300 capteurs
│   ├── server_centre.csv
│   ├── server_sud.csv
│   ├── server_ouest.csv
│   ├── server_est.csv
│   ├── all_sensors.csv      # Les 1 500 capteurs agrégés
│   └── large_sensors.csv    # 50 000 capteurs pour le stress test
└── results/                 # Fichiers JSON produits à l'exécution
    ├── classic_result.json
    ├── mapreduce_result.json
    ├── distributed_result.json
    ├── streaming_result.json
    ├── simulation_result.json
    └── benchmark.json
```

---

## Prérequis

Aucune dépendance externe. Python 3.8+ suffit (`multiprocessing`, `csv`, `json`, `tkinter` sont dans la stdlib).

```bash
git clone <url-du-repo>
cd map_reduce_groupe2
```

---

## Lancer le projet

### Option 1 — Tout d'un coup

```bash
python main.py
```

Lance dans l'ordre : génération des données, K-Means classique, MapReduce (chunk + multi-serveurs), streaming, benchmark.

### Option 2 — Dashboard graphique

```bash
python dashboard.py
```

Interface avec terminal intégré, onglet données et visualisation des clusters. On peut régler K, les itérations, les paramètres de streaming, et lancer chaque algo d'un clic.

---

### Exécution étape par étape

#### 1. Générer les données

```bash
python generate_data.py
```

#### 2. K-Means classique

```bash
python kmeans_classic.py --input data/all_sensors.csv --k 3
```

#### 3. Avec timeout (grand dataset)

```bash
python kmeans_classic.py --input data/large_sensors.csv --k 3 --timeout 30
```

#### 4. MapReduce — mode chunk

```bash
python kmeans_mapreduce.py data/all_sensors.csv --k 3 --max-iter 10
```

#### 5. MapReduce — mode multi-serveurs (scénario IoT)

```bash
python kmeans_mapreduce.py \
    --servers data/server_nord.csv data/server_centre.csv \
              data/server_sud.csv data/server_ouest.csv data/server_est.csv \
    --k 3 --max-iter 10
```

#### 6. Avec normalisation z-score

À activer quand les unités sont très différentes (ex. kbps vs °C) :

```bash
python kmeans_mapreduce.py data/all_sensors.csv --k 3 --normalize
```

#### 7. Streaming

```bash
python kmeans_streaming.py --waves 6 --sensors-per-wave 100 --k 3

# Avec délai entre vagues
python kmeans_streaming.py --waves 10 --sensors-per-wave 50 --delay 2.0
```

#### 8. Benchmark

```bash
python benchmark.py
```

#### 9. Simulation montée en charge

```bash
# Mode rapide — mesures réelles jusqu'à 1 000 000 capteurs
python simulation_comparison.py --dashboard

# Mode complet — paliers 1k–2M, timeout 15s sur le classique
python simulation_comparison.py
```

---

## Comment ça marche

À chaque itération du K-Means, l'affectation de chaque capteur à un cluster est indépendante des autres capteurs — c'est ce qui rend la parallélisation naturelle.

Chaque serveur régional exécute le Mapper sur ses propres données localement, sans envoyer les données brutes. Seules les sommes partielles (`cluster_id`, `somme`, `count`) transitent vers le Reducer central.

```
Serveur Nord   ──► MAP+COMBINE ──► (cluster_id, somme_partielle, count) ─┐
Serveur Centre ──► MAP+COMBINE ──► (cluster_id, somme_partielle, count)  │
Serveur Sud    ──► MAP+COMBINE ──► (cluster_id, somme_partielle, count) ─┼──► SHUFFLE ──► REDUCE
Serveur Ouest  ──► MAP+COMBINE ──► (cluster_id, somme_partielle, count)  │              (nouveaux centroïdes)
Serveur Est    ──► MAP+COMBINE ──► (cluster_id, somme_partielle, count) ─┘
```

Le Combiner local réduit le trafic réseau d'un facteur N/K : au lieu d'envoyer 300 enregistrements, chaque serveur envoie seulement K=3 sommes partielles.

---

## Résultats mesurés

| Dataset | Points | Classique | MapReduce | Ratio |
|---|---|---|---|---|
| Normal | 1 500 | 0.123 s | 0.124 s | ~égal — overhead quasi-nul |
| Stress test | 50 000 | 2.375 s | 1.004 s | **2.37× — MapReduce plus rapide** |
| Multi-serveurs | 1 500 | 0.074 s | 0.166 s | 2.24× plus lent sur ce volume |

Le mode multi-serveurs est plus lent sur 1 500 pts car chaque serveur ne traite que 300 capteurs — le coût du pool de processus n'est pas amorti. À partir de 10 000 capteurs, MapReduce est systématiquement plus rapide et devient la seule option viable à 500 000+ capteurs (l'algorithme classique dépasse le timeout de 15 s).

La SSE produite est identique dans les deux cas (`3 550 130.6`), ce qui confirme que la parallélisation ne dégrade pas la qualité du clustering.
