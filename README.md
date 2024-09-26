Projet de Gestion de Films
Description du Système
Ce projet est conçu pour gérer une base de données de films. Il utilise MongoDB pour stocker les informations sur les films, les réalisateurs, et d'autres entités pertinentes. Le système permet d'effectuer des opérations CRUD (Créer, Lire, Mettre à jour, Supprimer) sur les données des films et des réalisateurs.

Fonctionnalités Principales
Gestion des Films : Ajout, mise à jour, suppression et consultation des films.
Gestion des Réalisateurs : Ajout, mise à jour, suppression et consultation des réalisateurs.
Exportation des Données : Exportation de la base de données au format BSON.
Traitement des Données : Nettoyage et traitement des données avant l'importation dans la base de données.
Structure du Projet
bash
```
app/
├── utilities/
│   └── data_processor.py  # Fichier de traitement des données
├── persistence/
│   ├── database.py        # Gestion de la connexion à la base de données
│   └── ...                # Autres fichiers de persistance
├── schemas/
│   ├── film_schema.py     # Schémas Pydantic pour les films
│   └── director_schema.py  # Schémas Pydantic pour les réalisateurs
├── ...
```
Traitement Automatique des Vues
Tous les vues sont créées automatiquement via le fichier data_processor.py situé dans le paquet utilities. Ce fichier contient des fonctions qui traitent les données et génèrent des vues basées sur les agrégations des requêtes MongoDB. Cela permet d'automatiser la création des vues, garantissant ainsi que les données soient toujours à jour et accessibles rapidement.

Fonctionnalité du data_processor.py
Chargement des Données : Lit les données de la base de données et les prépare pour le traitement.
Création des Vues : Exécute des requêtes d'agrégation pour créer des vues basées sur les données existantes.
Gestion des Erreurs : Gère les erreurs potentielles lors du traitement des données, assurant ainsi l'intégrité des vues créées.
Installation
Clonez le repository :

```bash
Copy code
git clone [<url-du-repository>](https://github.com/bestacio89/PythonFilmsProcessor)
cd <nom-du-repository>
```
Installez les dépendances :


```bash

pip install -r requirements.txt
```
Configurez votre connexion à la base de données dans database.py.

Utilisation
Pour démarrer le système, exécutez le script principal. Assurez-vous que le serveur MongoDB est en cours d'exécution.

```bash
python main.py
```

Author Bernardo Estacio Abreu

License
Ce projet est sous licence MIT - voir le fichier LICENSE pour plus de détails.
