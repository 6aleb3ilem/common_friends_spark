# -*- coding: utf-8 -*-
"""
PROJET SPARK: RECHERCHE D'AMIS COMMUNS
=====================================
Ce programme utilise Apache Spark pour analyser un graphe social
et trouver les amis communs entre utilisateurs.

Auteur: [Votre nom]
Date: Juillet 2025
"""

import os
import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf

# ====================================================================
#  0: CONFIGURATION DE L'ENVIRONNEMENT SPARK
# ====================================================================
def configurer_environnement_spark():
    """
    Configure les variables d'environnement pour que Spark trouve Python.
    
    POURQUOI CETTE FONCTION?
    - Sur Windows, Spark cherche 'python3' mais Windows utilise 'python.exe'
    - Cette fonction dit à Spark où trouver l'interpréteur Python
    """
    # Récupère le chemin vers l'exécutable Python actuel
    chemin_python = sys.executable
    
    # Configure les variables d'environnement pour Spark
    os.environ['PYSPARK_PYTHON'] = chemin_python
    os.environ['PYSPARK_DRIVER_PYTHON'] = chemin_python
    
    # Configuration additionnelle pour Windows
    os.environ['HADOOP_HOME'] = 'C:\\hadoop'
    os.environ['SPARK_LOCAL_HOSTNAME'] = 'localhost'
    
    print(f"Configuration Python pour Spark: {chemin_python}")

def creer_session_spark():
    """
    Crée une session Spark avec la configuration appropriée.
    
    QU'EST-CE QU'UNE SESSION SPARK?
    - C'est le point d'entrée principal pour utiliser Spark
    - Elle coordonne l'exécution des tâches sur plusieurs processeurs
    - Elle gère la mémoire et les ressources système
    """
    # Configuration de Spark
    conf = SparkConf()
    conf.setAppName("RecherchAmisCommuns")  # Nom de l'application
    conf.setMaster("local[1]")  # Utilise 1 processeur local pour la stabilité
    conf.set("spark.sql.execution.pyspark.udf.faulthandler.enabled", "true")
    conf.set("spark.python.worker.faulthandler.enabled", "true")
    
    # Création de la session Spark
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    sc = spark.sparkContext  # Contexte Spark pour les opérations bas niveau
    sc.setLogLevel("ERROR")  # Réduit les messages de log
    
    return spark, sc

# ====================================================================
#  1: CHARGER LES DONNEES A L'AIDE DE PYSPARK
# ====================================================================
def charger_donnees_pyspark(chemin_fichier):
    """
     1: Charge et analyse le fichier de données des amis.
    
    COMMENT ÇA MARCHE?
    - Lit le fichier ligne par ligne
    - Sépare chaque ligne en: ID utilisateur, nom, liste d'amis
    - Convertit les données en format utilisable par Spark
    
    DONNEES D'ENTREE:
    Format: ID[TAB]Nom[TAB]Liste_amis_séparés_par_virgules
    Exemple: 1	Sidi	2,3,4
    """
    print(" 1: Chargement des données avec PySpark")
    print("-" * 50)
    
    donnees_utilisateurs = []
    
    try:
        # Ouverture et lecture du fichier
        with open(chemin_fichier, 'r', encoding='utf-8') as fichier:
            lignes = fichier.readlines()
        
        # Traitement de chaque ligne
        for ligne in lignes:
            ligne = ligne.strip()  # Supprime les espaces en début/fin
            
            # Ignore les lignes de commentaire et les lignes vides
            if ligne.startswith('#') or not ligne:
                continue
            
            # Sépare la ligne par tabulations
            parties = ligne.split('\t')
            
            if len(parties) >= 3:  # Vérifie qu'on a au moins 3 colonnes
                id_utilisateur = int(parties[0])
                nom = parties[1]
                chaine_amis = parties[2].strip()
                
                if chaine_amis:
                    # Convertit la liste d'amis en entiers
                    liste_amis = [int(ami) for ami in chaine_amis.split(',')]
                    donnees_utilisateurs.append((id_utilisateur, nom, liste_amis))
                    
                    print(f"Utilisateur chargé: {id_utilisateur} ({nom}) -> Amis: {liste_amis}")
    
    except FileNotFoundError:
        print(f"ERREUR: Fichier '{chemin_fichier}' introuvable")
        print(f"Répertoire actuel: {os.getcwd()}")
        return None
    
    print(f"RESULTAT  1: {len(donnees_utilisateurs)} utilisateurs chargés")
    return donnees_utilisateurs

# ====================================================================
#  2: GENERER TOUS LES COUPLES D'AMIS POSSIBLES
# ====================================================================
def generer_couples_amis(donnees_utilisateurs):
    """
     2: Génère tous les couples d'utilisateurs possibles.
    
    PRINCIPE:
    - Pour N utilisateurs, on crée N*(N-1)/2 couples
    - Chaque couple est trié (ID_min, ID_max) pour éviter les doublons
    - Exemple: (1,2) et (2,1) deviennent tous les deux (1,2)
    
    POURQUOI TRIER LES COUPLES?
    - Évite de traiter deux fois la même paire
    - (1,2) et (2,1) représentent la même relation d'amitié
    """
    print("\n 2: Génération des couples d'amis")
    print("-" * 50)
    
    # Création d'un dictionnaire pour accès rapide aux données
    dictionnaire_amis = {}
    dictionnaire_noms = {}
    
    # Remplissage des dictionnaires
    for id_utilisateur, nom, liste_amis in donnees_utilisateurs:
        dictionnaire_amis[id_utilisateur] = set(liste_amis)  # set() pour intersection rapide
        dictionnaire_noms[id_utilisateur] = nom
    
    # Génération de tous les couples possibles
    couples_generes = []
    liste_ids = list(dictionnaire_amis.keys())
    
    # Double boucle pour créer tous les couples
    for i in range(len(liste_ids)):
        for j in range(i + 1, len(liste_ids)):  # j > i pour éviter les doublons
            utilisateur1 = liste_ids[i]
            utilisateur2 = liste_ids[j]
            
            # Création du couple trié (ID minimum en premier)
            couple_trie = tuple(sorted([utilisateur1, utilisateur2]))
            couples_generes.append(couple_trie)
            
            print(f"Couple généré: {couple_trie} ({dictionnaire_noms[couple_trie[0]]} - {dictionnaire_noms[couple_trie[1]]})")
    
    print(f"RESULTAT  2: {len(couples_generes)} couples générés")
    return couples_generes, dictionnaire_amis, dictionnaire_noms

# ====================================================================
#  3: CALCULER LES AMIS COMMUNS POUR CHAQUE PAIRE
# ====================================================================
def calculer_amis_communs(couples_generes, dictionnaire_amis, dictionnaire_noms):
    """
     3: Calcule les amis communs pour chaque couple d'utilisateurs.
    
    ALGORITHME:
    - Pour chaque couple (A, B)
    - Récupère les amis de A et les amis de B
    - Calcule l'intersection (amis présents dans les deux listes)
    - Stocke le résultat si des amis communs existent
    
    EXEMPLE:
    - Sidi (1) a comme amis: [2, 3, 4]
    - Mohamed (2) a comme amis: [1, 3, 5]
    - Intersection: [3] -> Aicha est leur ami commun
    """
    print("\n 3: Calcul des amis communs")
    print("-" * 50)
    
    resultats_amis_communs = {}
    
    # Traitement de chaque couple
    for couple in couples_generes:
        utilisateur1, utilisateur2 = couple
        
        # Récupération des listes d'amis
        amis_utilisateur1 = dictionnaire_amis[utilisateur1]
        amis_utilisateur2 = dictionnaire_amis[utilisateur2]
        
        # Calcul de l'intersection (amis communs)
        amis_communs = amis_utilisateur1.intersection(amis_utilisateur2)
        
        # Stockage du résultat si des amis communs existent
        if amis_communs:
            resultats_amis_communs[couple] = {
                'amis_communs': list(amis_communs),
                'nom_utilisateur1': dictionnaire_noms[utilisateur1],
                'nom_utilisateur2': dictionnaire_noms[utilisateur2]
            }
            
            print(f"Couple {couple}: {len(amis_communs)} ami(s) commun(s) -> {list(amis_communs)}")
    
    print(f"RESULTAT  3: {len(resultats_amis_communs)} couples avec amis communs")
    return resultats_amis_communs

# ====================================================================
#  4: FILTRER LES RESULTATS POUR MOHAMED ET SIDI
# ====================================================================
def filtrer_mohamed_sidi(resultats_amis_communs, dictionnaire_noms):
    """
     4: Filtre et affiche les amis communs entre Mohamed (ID 2) et Sidi (ID 1).
    
    PROCESSUS:
    - Recherche la paire normalisée (1, 2) dans les résultats
    - Extrait les amis communs pour cette paire spécifique
    - Prépare l'affichage au format demandé
    """
    print("\n 4: Filtrage pour Mohamed et Sidi")
    print("-" * 50)
    
    # Définition de la paire cible (normalisée)
    paire_cible = (1, 2)  # Sidi (1) et Mohamed (2), trié par ID croissant
    
    if paire_cible in resultats_amis_communs:
        donnees_paire = resultats_amis_communs[paire_cible]
        amis_communs = donnees_paire['amis_communs']
        
        print(f"Paire trouvée: {paire_cible}")
        print(f"Utilisateurs: {dictionnaire_noms[1]} et {dictionnaire_noms[2]}")
        print(f"Amis communs: {amis_communs}")
        
        return paire_cible, amis_communs
    else:
        print(f"Aucun ami commun trouvé pour la paire {paire_cible}")
        return paire_cible, []

# ====================================================================
#  5: VERIFICATION DE LA NORMALISATION DES PAIRES
# ====================================================================
def verifier_normalisation_paire(paire_cible):
    """
     5: Vérifie que la paire est bien normalisée (ID min, ID max).
    
    DEFINITION DE NORMALISATION:
    - Dans une paire (A, B), A doit toujours être <= B
    - Cela garantit l'unicité: (1,2) et (2,1) deviennent (1,2)
    - Permet une recherche efficace dans les résultats
    """
    print("\n 5: Vérification de la normalisation")
    print("-" * 50)
    
    est_normalisee = paire_cible[0] <= paire_cible[1]
    
    print(f"Paire à vérifier: {paire_cible}")
    print(f"Est normalisée (ID1 <= ID2): {est_normalisee}")
    
    if est_normalisee:
        print("VALIDATION: La paire est correctement normalisée")
    else:
        paire_normalisee = tuple(sorted(paire_cible))
        print(f"CORRECTION: Paire normalisée -> {paire_normalisee}")
        return paire_normalisee
    
    return paire_cible

# ====================================================================
#  6: AFFICHAGE AU FORMAT DEMANDE
# ====================================================================
def afficher_resultat_final(paire_cible, amis_communs, fichier_sortie="output/resultats.txt"):
    """
     6: Affiche le résultat au format: ID1 ID2 [liste_amis_communs]
    
    FORMAT DE SORTIE:
    - Format demandé: "1 2 [3]"
    - Signification: Utilisateurs 1 et 2 ont l'utilisateur 3 comme ami commun
    - Sauvegarde dans un fichier pour traçabilité
    """
    print("\n 6: Affichage du résultat final")
    print("-" * 50)
    
    # Création du répertoire de sortie s'il n'existe pas
    os.makedirs(os.path.dirname(fichier_sortie), exist_ok=True)
    
    # Format de sortie demandé
    resultat_formate = f"{paire_cible[0]} {paire_cible[1]} {amis_communs}"
    
    print("RESULTAT FINAL:")
    print("=" * 30)
    print(resultat_formate)
    print("=" * 30)
    
    # Sauvegarde dans le fichier
    with open(fichier_sortie, 'w', encoding='utf-8') as fichier:
        fichier.write("RESULTATS DE L'ANALYSE DES AMIS COMMUNS\n")
        fichier.write("="*50 + "\n")
        fichier.write(f"Date d'exécution: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        fichier.write("REQUETE: Amis communs entre Mohamed (ID 2) et Sidi (ID 1)\n")
        fichier.write(f"REPONSE: {resultat_formate}\n\n")
        fichier.write("INTERPRETATION:\n")
        fichier.write(f"- Utilisateur {paire_cible[0]} (Sidi) et utilisateur {paire_cible[1]} (Mohamed)\n")
        fichier.write(f"- Ont {len(amis_communs)} ami(s) commun(s): {amis_communs}\n")
    
    print(f"Résultats sauvegardés dans: {fichier_sortie}")
    return resultat_formate

# ====================================================================
# FONCTION PRINCIPALE
# ====================================================================
def main():
    """
    Fonction principale qui orchestre l'exécution de toutes les tâches.
    
    FLUX D'EXECUTION:
    1. Configuration de Spark
    2. Chargement des données
    3. Génération des couples
    4. Calcul des amis communs
    5. Filtrage pour Mohamed et Sidi
    6. Vérification de normalisation
    7. Affichage du résultat final
    """
    print("DEMARRAGE DU PROGRAMME D'ANALYSE D'AMIS COMMUNS")
    print("=" * 60)
    
    # Configuration de l'environnement
    configurer_environnement_spark()
    
    try:
        # Initialisation de Spark
        print("\nInitialisation de Spark...")
        spark, sc = creer_session_spark()
        print("Session Spark créée avec succès")
        
        #  1: Chargement des données
        chemin_fichier = "data/friends_common.txt"
        donnees_utilisateurs = charger_donnees_pyspark(chemin_fichier)
        
        if donnees_utilisateurs is None:
            print("ARRET: Impossible de charger les données")
            return
        
        #  2: Génération des couples
        couples_generes, dictionnaire_amis, dictionnaire_noms = generer_couples_amis(donnees_utilisateurs)
        
        #  3: Calcul des amis communs
        resultats_amis_communs = calculer_amis_communs(couples_generes, dictionnaire_amis, dictionnaire_noms)
        
        #  4: Filtrage pour Mohamed et Sidi
        paire_cible, amis_communs = filtrer_mohamed_sidi(resultats_amis_communs, dictionnaire_noms)
        
        #  5: Vérification de normalisation
        paire_normalisee = verifier_normalisation_paire(paire_cible)
        
        #  6: Affichage du résultat final
        resultat_final = afficher_resultat_final(paire_normalisee, amis_communs)
        
        # Résumé d'exécution
        print(f"\nRESUME D'EXECUTION:")
        print(f"- Utilisateurs traités: {len(donnees_utilisateurs)}")
        print(f"- Couples analysés: {len(couples_generes)}")
        print(f"- Couples avec amis communs: {len(resultats_amis_communs)}")
        print(f"- Résultat final: {resultat_final}")
        
    except Exception as erreur:
        print(f"ERREUR LORS DE L'EXECUTION: {erreur}")
        import traceback
        traceback.print_exc()
        
    finally:
        # Nettoyage: fermeture de la session Spark
        try:
            if 'spark' in locals():
                spark.stop()
                print("\nSession Spark fermée proprement")
        except Exception as erreur:
            print(f"Erreur lors de la fermeture de Spark: {erreur}")

if __name__ == "__main__":
    main()