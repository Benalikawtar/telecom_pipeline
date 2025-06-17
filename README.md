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

## 🗃️ Structure du projet

Le projet est organisé de manière modulaire pour séparer les différentes étapes du pipeline :

- `generate_cdr.py` : Génération de fichiers CDR/EDR réalistes avec anomalies
- `producer.py` : Envoi des données vers Kafka (topic `telecom_cdr_topic`)
- `stream_mediation.py` : Médiation en streaming avec Spark, séparation clean/error
- `rating_engine.py` : Moteur de tarification batch avec PostgreSQL
- `billing_engine.py` : Agrégation des coûts et génération des factures
- `docker-compose.yml` : Configuration des services Kafka, Spark, PostgreSQL
- `telecom_records.json` : Fichier d’exemple des CDR simulés
- `checkpoints/` : Répertoires de sauvegarde Spark pour tolérance aux pannes
- `lib/` : Fichiers JAR nécessaires à la connexion PostgreSQL


