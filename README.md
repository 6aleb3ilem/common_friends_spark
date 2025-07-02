# Projet Spark: Recherche d'Amis Communs

## Objectif du Projet

Ce projet implémente un système de découverte d'amis communs dans un graphe social en utilisant Apache Spark et PySpark. Il démontre l'application des concepts MapReduce et de calcul distribué pour l'analyse de réseaux sociaux.

## Apache Spark?

Apache Spark est un moteur de calcul distribué qui permet de traiter de grandes quantités de données sur plusieurs ordinateurs en même temps. Imaginez-le comme un chef d'orchestre qui coordonne plusieurs musiciens (processeurs) pour jouer une symphonie (traiter des données) de manière harmonieuse.

- **Rapidité**: Traite les données en mémoire, beaucoup plus rapide que les solutions traditionnelles
- **Simplicité**: Interface de programmation intuitive
- **Polyvalence**: Gère différents types d'analyses (batch, streaming, machine learning)
- **Scalabilité**: Fonctionne aussi bien sur un ordinateur portable que sur des milliers de serveurs


## Description du Problème

### Contexte
Dans un réseau social, nous voulons identifier les amis communs entre deux utilisateurs spécifiques. 
### Données d'Entrée
Le fichier `friends_common.txt` contient:
- **Format**: ID[TAB]Nom[TAB]Liste_amis_séparés_par_virgules
- **Exemple**: `1	Sidi	2,3,4` signifie que Sidi (ID 1) est ami avec les utilisateurs 2, 3 et 4

### Relations d'Amitié
Les amitiés sont **mutuelles**: si A est ami avec B, alors B est ami avec A.

## Architecture de la Solution

### Approche MapReduce
Notre solution suit le paradigme MapReduce:

1. **Map**: Transformation des données en couples clé-valeur
2. **Reduce**: Agrégation des résultats pour calculer les intersections

### Étapes de Traitement

#### 1. Chargement des Données (Map)
- Lecture du fichier texte
- Parsing de chaque ligne
- Création d'une structure de données utilisable

#### 2. Génération des Couples (Map)
- Création de tous les couples d'utilisateurs possibles
- Normalisation (ID_min, ID_max) pour éviter les doublons

#### 3. Calcul des Amis Communs (Reduce)
- Pour chaque couple, intersection des listes d'amis
- Utilisation d'opérations ensemblistes pour l'efficacité

#### 4. Filtrage des Résultats
- Extraction des résultats pour la paire spécifique demandée
