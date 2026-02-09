# BirdPipe – Observation Pipeline (Project 2)

This project implements a data pipeline for bird audio observations using Python,
Docker, MongoDB, MinIO, Kafka and an external bird classification API.

## Architecture Overview

Audio files are processed through the following pipeline:

1. Local audio files (`data/audio`)
2. Upload to MinIO (object storage)
3. Metadata stored in MongoDB
4. Audio classified using external Bird Classification API
5. Classification results stored in MongoDB
6. Aggregated analytical CSV report generated

## Technology Stack

- Python 3.x
- Docker & Docker Compose
- MongoDB
- MinIO (S3-compatible storage)
- Apache Kafka
- External bird classification API



birdpipe-observation-pipeline/
├── data/
│ └── audio/
│ ├── audio_bird_001.wav
│ ├── audio_bird_002.wav
│ └── audio_bird_003.wav
├── src/
│ ├── healthcheck_services.py
│ ├── upload_audio.py
│ ├── classify_audio.py
│ ├── ingest_observations.py
│ └── generate_report.py
├── out/
│ └── bird_report_*.csv
├── docker-compose.yml
├── .env
└── README.md


---

## Konfiguracija (.env)

```env
MONGO_URI=mongodb://root:rootpass@localhost:27017/?authSource=admin
MONGO_DB=birdpipe

MINIO_ENDPOINT=127.0.0.1:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
MINIO_BUCKET=ornitho-archive

KAFKA_BOOTSTRAP=localhost:9092
KAFKA_TOPIC=observations
KAFKA_GROUP=birdpipe_ingest
KAFKA_MAX_SECONDS=8
KAFKA_TIMEOUT_MS=6000

AUDIO_DIR=.\data\audio
AUDIO_LAT=45.8150
AUDIO_LON=15.9819

CLASSIFY_URL=https://aves.regoch.net/api/classify

Pokretanje projekta
1. Pokretanje infrastrukture
docker compose up -d


Pokreće:

MongoDB

MinIO

Kafka (+ Zookeeper)

2. Provjera servisa (Health check)
python src/healthcheck_services.py


Provjerava:

MongoDB konekciju i write operacije

MinIO konekciju i bucket

osnovnu dostupnost svih servisa

3. Upload audio datoteka
python src/upload_audio.py


Rezultat:

audio datoteke se spremaju u MinIO bucket

metapodaci se spremaju u MongoDB kolekciju audio_files

4. Klasifikacija audio zapisa
python src/classify_audio.py


Rezultat:

audio zapisi se šalju vanjskom API-ju za klasifikaciju

rezultati (raw segmenti s confidence score-om) se spremaju u MongoDB kolekciju classifications

svaki audio zapis ima više vremenskih segmenata

5. Kafka ingest (field observations)
python src/ingest_observations.py


Rezultat:

Kafka poruke se konzumiraju

opažanja se spremaju u MongoDB kolekciju field_observations

6. Generiranje analitičkog izvještaja (CSV)
Izvještaj za sve vrste
python src/generate_report.py --min-score 0.3

Filtrirani izvještaj po vrsti (fuzzy search)
python src/generate_report.py --min-score 0.3 --species "motacilla"


CSV izvještaji se generiraju u direktorij out/.
