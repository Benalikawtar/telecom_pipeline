# 📡 Telecom Data Pipeline – Big Data Project

Ce projet simule le traitement des données techniques d’un opérateur télécom (CDR/EDR) à travers un pipeline complet, en utilisant les technologies Big Data modernes.

## 🎯 Objectif

Construire un pipeline capable de gérer la génération, la médiation, la tarification et la facturation des enregistrements d’usage (voix, SMS, data), en temps réel ou en batch.

## 🔧 Étapes du pipeline

### 1. 🔁 Génération des données
- Génération de CDR/EDR réalistes (voix, SMS, data)
- Distribution configurable (ex. 60 % voix, 30 % data, 10 % SMS)
- Injection d’anomalies : champs manquants, corrompus, doublons, timestamps désordonnés
- Sortie vers fichier JSON ou Kafka (`telecom_cdr_topic`)

### 2. ⚙️ Médiation en streaming (Spark Streaming)
- Lecture depuis Kafka
- Parsing des enregistrements JSON
- Extraction du `msisdn` depuis `caller_id`, `sender_id` ou `user_id`
- Vérification d’erreurs et statuts (`normal`, `error`)
- Suppression des doublons
- Séparation : envoi vers `clean_cdr_topic` ou `error_cdr_topic`

### 3. 💰 Moteur de tarification (Batch)
- Lecture des données propres depuis Kafka
- Jointure avec les tables PostgreSQL : `customers`, `products`, `rate_plans`, `product_rates`
- Application des règles tarifaires selon le type de service, la zone, les promotions, le statut client
- Calcul du coût, gestion des statuts (`rated`, `rejected`, `unmatched`, `error`)
- Insertion dans la table `usage_records`

### 4. 🧾 Moteur de facturation (Batch)
- Agrégation mensuelle des coûts par client
- Application des taxes, quotas, remises
- Génération de factures (format JSON)
- Sauvegarde dans la table `bills` (ou export)

## 🛠️ Stack technique

- **Python** – traitement et scripts
- **Apache Spark 3.4.2** – streaming et batch
- **Apache Kafka** – ingestion en temps réel
- **PostgreSQL** – stockage des métadonnées et résultats
- **Docker / WSL Ubuntu** – environnement d’exécution

## 🗃️ Structure des répertoires

├── generate_cdr.py # Génération des fichiers CDR/EDR
├── producer.py # Envoi vers Kafka
├── stream_mediation.py # Médiation avec Spark Streaming
├── rating_engine.py # Tarification (batch)
├── billing_engine.py # Facturation (batch)
├── checkpoints/ # Checkpoints Spark
├── telecom_records.json # Données simulées
├── docker-compose.yml # Services Kafka, Spark, Postgres
└── lib/ # Connecteurs PostgreSQL

