#!/usr/bin/env bash
set -e
echo "Starting TimescaleDB and Grafana via docker compose..."
docker compose up -d
echo "Services started. Check logs with: docker compose logs -f"
echo "Make sure your Kafka is running and accessible via KAFKA_BOOTSTRAP_SERVERS in .env"
