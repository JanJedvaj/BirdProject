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

## Project Structure

