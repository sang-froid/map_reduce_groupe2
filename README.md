# K-Means MapReduce — Clustering de capteurs IoT

**TP4 Big Data · Master 1 GL · Groupe 2 · IFRI 2025-2026**

---

## Contexte

On dispose de 5 serveurs régionaux (Nord, Centre, Sud, Ouest, Est), chacun collectant en continu les mesures de 300 capteurs IoT (température, humidité, vibration, trafic réseau). L'objectif est de regrouper ces capteurs en zones homogènes via K-Means, en comparant deux approches : séquentielle classique et MapReduce parallèle distribué.

---

## Structure du projet

```
map_reduce_groupe2/
├── dashboard.py             # Interface Tkinter — dark theme
├── generate_data.py         # Génère les données simulées des 5 serveurs
├── kmeans_classic.py        # K-Means séquentiel avec K-Means++ et timeout
├── kmeans_mr_job.py         # Le cœur : Mapper, Combiner, Shuffle, Reducer
├── kmeans_mapreduce.py      # Orchestrateur — mode chunk ou multi-serveurs
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

```bash
python dashboard.py
```

Interface avec terminal intégré, onglet données et visualisation des clusters. On peut régler K, les itérations, et lancer chaque algo (K-Means classique, MapReduce, benchmark, simulation) d'un clic.

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
| Normal | 1 500 | 0.072 s | 0.125 s | overhead 1.74× (coût fixe pool) |
| Stress test | 50 000 | 2.417 s | 1.002 s | **2.41× — MapReduce plus rapide** |
| Multi-serveurs | 1 500 | 0.071 s | 0.141 s | overhead 1.99× (normal à ce volume) |

Le mode multi-serveurs est plus lent sur 1 500 pts car chaque serveur ne traite que 300 capteurs — le coût du pool de processus n'est pas amorti. MapReduce devient avantageux dès 50 000 capteurs et devient la seule option viable à 500 000+ capteurs (l'algorithme classique dépasse le timeout de 20 s dans `benchmark.py`).

La SSE produite est identique dans les deux cas (`3 550 130.6`), ce qui confirme que la parallélisation ne dégrade pas la qualité du clustering.
