# Changelog

All notable changes to this project are documented here.

## [1.0.0] - 2026-05-19

### Added
- Kafka 3.5 producer fetching live weather data every 30 seconds for 12 cities across 6 continents
- Pydantic v2 schema validation before data enters Kafka
- Consumer group with manual offset commits
- Dead Letter Queue table for failed message handling
- Flask REST API exposing processed data
- PostgreSQL 15 persistent storage with Kafka offset tracking
- Zookeeper cluster management
- Kafka UI monitoring at localhost:8080
- Health check module for Kafka, DB and API
- Docker Compose stack with 8 containers
- 20+ pytest unit tests
- GitHub Actions CI pipeline
- Makefile with shortcuts
- Mermaid architecture diagram in README
- 2207+ messages streamed, 0 DLQ failures in live run

## [1.0.1] - 2026-06-01

### Improved
- Added project roadmap: next steps include Delta Lake integration and Spark Structured Streaming consumer
