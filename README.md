# GeoRescue

GeoRescue est un prototype de supervision des alertes sismiques et d'aide à la décision pour les secours. Il rassemble une carte web, une API, une base de données géospatiale PostGIS, un bus d'événements Kafka, un simulateur de séismes et un consommateur qui évalue les hôpitaux proches d'une alerte.

> **Périmètre du prototype :** les séismes sont simulés autour de Casablanca. Le script `processor/spark_sedona_job.py` consomme les messages Kafka et exécute les requêtes spatiales dans PostGIS ; malgré son nom, il ne lance ni Apache Spark ni Apache Sedona.

## Fonctionnalités

- Affichage des hôpitaux de démonstration sur une carte Leaflet.
- Publication d'alertes de test via Kafka, environ toutes les cinq secondes.
- Enregistrement des alertes et de leurs coordonnées dans PostGIS.
- Affichage des cinq alertes les plus récentes et de leur zone d'impact calculée sur un rayon de 20 km.
- Recherche des trois hôpitaux les plus proches d'une alerte et calcul de leur distance.
- Affichage d'une liste des secours à dépêcher dans le tableau de bord.

## Architecture

```text
simulateur/producer.py
        │ messages JSON, topic « seismes »
        ▼
      Kafka ◄──────────── processor/spark_sedona_job.py
                              │ écrit l'alerte et recherche les hôpitaux
                              ▼
                        PostgreSQL + PostGIS
                              ▲
                              │ requêtes REST et données GeoJSON
                        API NestJS
                              │
                              ▼
                 Tableau Leaflet dans le navigateur
```

Les services PostgreSQL/PostGIS, Kafka et ZooKeeper sont déclarés dans [`docker-compose.yml`](./docker-compose.yml). L'initialisation de la base et des hôpitaux de démonstration est décrite dans [`init.sql`](./init.sql).

## Prérequis

- Windows 10/11 avec PowerShell (ou des commandes équivalentes sur macOS/Linux).
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) avec Docker Compose.
- [Node.js](https://nodejs.org/) et npm.
- Python 3.12 recommandé pour le processeur et le simulateur.
- Les ports locaux `2181`, `3000`, `5435` et `9092` disponibles.
- Une connexion Internet pour charger les tuiles OpenStreetMap et les ressources Leaflet du tableau de bord.

## Démarrage local

Ouvrir des terminaux dans le dossier du projet. Les commandes ci-dessous sont prévues pour PowerShell, depuis la racine `Projet`.

### 1. Démarrer PostgreSQL/PostGIS et Kafka

Créer une configuration locale à partir du modèle, puis remplacer le mot de passe d'exemple par un mot de passe réservé à cette installation :

```powershell
Copy-Item .env.example .env
notepad .env
```

Le fichier `.env` est ignoré par Git. Ne le publiez pas et n'utilisez pas ces identifiants pour une base partagée ou de production.

```powershell
docker compose up -d
docker compose ps
```

Le service de base de données est accessible depuis Windows à `localhost:5435` par défaut (PostgreSQL écoute sur `5432` à l'intérieur du conteneur). Kafka est accessible à `localhost:9092`.

Au premier démarrage, Docker exécute `init.sql`, crée les tables et ajoute quatre hôpitaux de démonstration à Casablanca et Rabat. Ce script d'initialisation ne réinitialise pas automatiquement une base déjà créée.

### 2. Installer et lancer l'API

Dans un nouveau terminal :

```powershell
Set-Location .\backend
npm install
Get-Content ..\.env | ForEach-Object {
    if ($_ -match '^\s*([^#=]+)\s*=\s*(.*)\s*$') {
        [Environment]::SetEnvironmentVariable($matches[1].Trim(), $matches[2].Trim(), 'Process')
    }
}
npm run start:dev
```

L'API et le tableau de bord sont servis à l'adresse **http://localhost:3000**. Le mode `start:dev` redémarre le serveur lorsque les fichiers TypeScript surveillés changent.

### 3. Installer et lancer le processeur Kafka

Dans un autre terminal, depuis la racine du projet :

```powershell
Set-Location .\processor
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
Get-Content ..\.env | ForEach-Object {
    if ($_ -match '^\s*([^#=]+)\s*=\s*(.*)\s*$') {
        [Environment]::SetEnvironmentVariable($matches[1].Trim(), $matches[2].Trim(), 'Process')
    }
}
python .\spark_sedona_job.py
```

Le script reste à l'écoute du topic Kafka `seismes`. Il enregistre chaque message reçu dans `alertes_sismiques`, puis affiche les hôpitaux situés à moins de 20 km de l'épicentre.

### 4. Démarrer le simulateur d'alertes

Dans un quatrième terminal, depuis la racine du projet :

```powershell
Set-Location .\simulateur
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
python .\producer.py
```

Le simulateur envoie une alerte aléatoire autour de Casablanca toutes les cinq secondes. Quand le processeur la reçoit, la nouvelle alerte apparaît sur la carte et dans la liste du tableau de bord.

Pour interrompre le simulateur ou le processeur, utiliser `Ctrl+C` dans leur terminal.

## API REST

L'API utilise le préfixe `/api` et écoute sur le port `3000`.

| Méthode | Route | Description |
|---|---|---|
| `GET` | `/api/hopitaux` | Renvoie les hôpitaux et leur géométrie au format GeoJSON. |
| `GET` | `/api/alertes` | Renvoie au maximum les cinq alertes les plus récentes avec leur géométrie et leur buffer d'impact de 20 km. |
| `GET` | `/api/dispatch?lat=<latitude>&lon=<longitude>` | Renvoie les trois hôpitaux les plus proches et leur distance en kilomètres. |

Exemples dans PowerShell :

```powershell
Invoke-RestMethod http://localhost:3000/api/hopitaux
Invoke-RestMethod http://localhost:3000/api/alertes
Invoke-RestMethod 'http://localhost:3000/api/dispatch?lat=33.589&lon=-7.618'
```

## Structure du projet

```text
.
├── backend/
│   ├── public/index.html       # Tableau de bord Leaflet
│   ├── src/                    # API NestJS et accès à PostGIS
│   └── package.json
├── processor/
│   ├── spark_sedona_job.py     # Consommateur Kafka et requêtes PostGIS
│   └── requirements.txt
├── simulateur/
│   ├── producer.py             # Producteur Kafka d'alertes de test
│   └── requirements.txt
├── docker-compose.yml          # PostgreSQL/PostGIS, Kafka et ZooKeeper
└── init.sql                    # Schéma spatial et données de démonstration
```

## Configuration et données

Les paramètres de connexion sont lus dans le fichier local `.env`. Le modèle `.env.example` indique les valeurs initiales attendues : base `georescue`, utilisateur `user`, hôte `localhost` et port publié `5435`.

`POSTGRES_PASSWORD` doit être défini avant le démarrage de Docker, de l'API et du processeur. Compose lit `.env` automatiquement ; les commandes ci-dessus chargent aussi ses valeurs dans l'environnement des processus lancés depuis leurs terminaux. N'ajoutez jamais `.env` à Git. En production, utilisez un gestionnaire de secrets et protégez l'accès réseau à PostgreSQL et Kafka.

Les données de la base vivent dans un volume Docker. `docker compose down` arrête et supprime les conteneurs du projet, mais conserve les données du volume ; évitez `docker compose down -v` si vous souhaitez les garder.

## Dépannage

- **Échec de connexion à PostgreSQL :** vérifier que le conteneur `db` est actif avec `docker compose ps` et que le port `5435` est disponible. La configuration de l'API et du processeur doit correspondre au port publié dans `docker-compose.yml`.
- **Échec de connexion à Kafka :** démarrer la pile avec `docker compose up -d` et vérifier que Kafka est prêt avant de lancer le producteur et le processeur.
- **Aucune alerte sur la carte :** vérifier que le simulateur et le processeur sont lancés, puis consulter `/api/alertes`. Les alertes ne sont créées dans la base qu'après réception par le processeur.
- **La carte est vide ou sans fond cartographique :** ouvrir le tableau de bord à `http://localhost:3000` et vérifier l'accès Internet aux ressources Leaflet et aux tuiles cartographiques.
- **Erreur TypeScript :** exécuter les commandes depuis `backend` et vérifier l'installation avec `npm install`.

## Vérification rapide

Depuis `backend`, la compilation TypeScript peut être vérifiée sans générer de fichiers :

```powershell
.\node_modules\.bin\tsc.cmd --noEmit
```

## Licence

Aucune licence open source n'est déclarée pour le moment. Sauf ajout explicite d'une licence, les droits d'utilisation et de redistribution restent réservés aux auteurs.
