# Agent Instructions

This repository is a demo of Apache Jena Fuseki + TDB2 horizontal read scaling with Kafka as the RDF event log and Locust as the load generator.

Use this file as the operational runbook for coding agents. The README explains the architecture; this file focuses on commands, checks, and expected behavior.

## Project Shape

- Python package: `src/jena_demo_scale`
- CLI scripts are defined in `pyproject.toml`
- Compose stack: `docker-compose.yml`
- Load test: `locustfile.py`
- Fuseki image: `Dockerfile.fuseki`
- Locust image: `Dockerfile.locust`
- Fuseki config: `fuseki/config-reader.ttl`
- Ontology seed/model: `ontology/demo.ttl`

## Requirements

Required host tools:

- Docker with Docker Compose v2
- `uv`
- Python 3.13 support through `uv`

Expected free host ports with default Compose settings:

- `8080`: read-balanced SPARQL endpoint through Nginx
- `8089`: Locust UI
- `8090`: Kafka UI
- `9092`: Kafka bootstrap from host

These can be changed with Compose environment variables such as `NGINX_HOST_PORT`, `LOCUST_HOST_PORT`, `KAFKA_UI_HOST_PORT`, and `KAFKA_HOST_PORT`. If changing `KAFKA_HOST_PORT`, Compose also updates Kafka's advertised host listener.

## Configuration Overrides

The Compose file has defaults for local demo use. Override these only when needed:

- `KAFKA_VERSION`, default `4.3.1`
- `JENA_VERSION`, default `6.2.0`
- `FUSEKI_KAFKA_VERSION`, default `3.1.0`
- `KAFKA_TOPIC`, default `rdf-events`
- `KAFKA_DLQ_TOPIC`, default `rdf-events.dlq`
- `KAFKA_MEM_LIMIT`, default `450m`
- `FUSEKI_MEM_LIMIT`, default `2g`
- `KAFKA_HOST_PORT`, default `9092`
- `NGINX_HOST_PORT`, default `8080`
- `LOCUST_HOST_PORT`, default `8089`
- `KAFKA_UI_HOST_PORT`, default `8090`

For normal validation, use the defaults. If overriding ports, update any host-side command URLs accordingly.

## Setup

Install Python dependencies:

```bash
uv sync
```

Build the custom images:

```bash
docker compose build fuseki-reader locust
```

Start the stack with two Fuseki reader replicas:

```bash
docker compose up -d --scale fuseki-reader=2
```

Check service status:

```bash
docker compose ps
```

Expected state:

- `kafka` is healthy
- `fuseki-reader` replicas are healthy
- `nginx-read` is healthy
- `locust` is running
- `kafka-ui` is running

## Important Endpoints

- Read-balanced SPARQL endpoint: `http://localhost:8080/ds/sparql`
- Locust UI: `http://localhost:8089`
- Kafka UI: `http://localhost:8090`
- Kafka bootstrap from host: `localhost:9092`
- Kafka bootstrap from containers: `kafka:29092`

## Smoke Test

Produce a small amount of RDF data:

```bash
uv run jena-demo-produce --events 25 --rate 25
```

Verify the replicated request count through the read-balanced SPARQL endpoint:

```bash
uv run jena-demo-stats --once
```

Expected output after replication catches up:

```text
requests=25
```

Replication can take a few seconds. If the count is lower than expected, wait briefly and run the stats command again.

## Load Test

Run a short headless mixed write/read load test:

```bash
uv run locust -f locustfile.py --headless --users 8 --spawn-rate 8 --run-time 20s -H http://localhost:8080
```

Expected result:

- Locust exits successfully
- Failure count is `0`
- The summary includes `KAFKA produce rdf-events`
- The summary includes SPARQL read requests

For interactive testing, open the Locust UI:

```bash
open http://localhost:8089
```

If `open` is unavailable or requires GUI access, report the URL instead of trying to launch a browser.

## Scale Readers

Scale the read tier from two to four Fuseki replicas:

```bash
docker compose up -d --scale fuseki-reader=4
```

Then verify:

```bash
docker compose ps
uv run jena-demo-stats --once
```

Each Fuseki reader must use a unique Kafka consumer group. This is configured through `ENV_FUSEKI_GROUP_ID` in `docker-compose.yml`, using the container hostname. Do not change this to a shared static group unless intentionally testing broken replication.

## Common Development Commands

Run the producer manually:

```bash
uv run jena-demo-produce --events 1000 --rate 100
```

Poll stats continuously:

```bash
uv run jena-demo-stats
```

Run stats against an explicit endpoint:

```bash
uv run jena-demo-stats --endpoint http://localhost:8080/ds/sparql --once
```

Compile-check Python files:

```bash
python -m py_compile src/jena_demo_scale/*.py locustfile.py
```

## Stop Or Reset

Stop containers but keep volumes:

```bash
docker compose down
```

Reset all local compose state, including Kafka persisted data:

```bash
docker compose down -v
```

Use `down -v` only when a clean run is needed, because it deletes the local Kafka topic data for this compose project.

## Troubleshooting

If the producer cannot connect to Kafka:

```bash
docker compose ps kafka
docker compose logs kafka
```

If SPARQL reads fail:

```bash
docker compose ps nginx-read fuseki-reader
docker compose logs nginx-read
docker compose logs fuseki-reader
```

If request counts are lower than expected:

- wait a few seconds and rerun `uv run jena-demo-stats --once`
- check Fuseki reader logs for Kafka consumer errors
- confirm the readers do not share the same Kafka consumer group

If Docker image build fails while downloading Maven, Jena, or Python dependencies, it is usually a network or registry availability problem. Retry the build before changing project code.

## Agent Notes

- Prefer `rg` for searching files.
- Do not edit generated lock data unless dependency changes require it.
- Keep README architecture explanations intact unless the user explicitly asks to update them.
- For behavior changes, update or add focused verification commands in this file when useful.
