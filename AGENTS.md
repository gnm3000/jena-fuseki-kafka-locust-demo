# Agent Instructions

This repository is a demo of Apache Jena Fuseki + TDB2 horizontal read scaling with a single Fuseki **writer** as the source of truth, RDF Delta as the replication transport to read replicas, and Locust as the load generator.

Use this file as the operational runbook for coding agents. The README explains the architecture; this file focuses on commands, checks, and expected behavior.

## Project Shape

- Python package: `src/jena_demo_scale` (`producer.py`, `stats.py`, `gateway.py`, `rdf.py`)
- CLI scripts are defined in `pyproject.toml`
- Compose stack: `docker-compose.yml`
- Load test: `locustfile.py`
- Fuseki image (writer and reader, same image, different config): `Dockerfile.fuseki`
- RDF Delta Patch Log Server image: `Dockerfile.delta-server`
- Write gateway image: `Dockerfile.gateway`
- Locust image: `Dockerfile.locust`
- Fuseki configs: `fuseki/config-writer.ttl` (has an update endpoint), `fuseki/config-reader.ttl` (read-only)
- Ontology seed/model: `ontology/demo.ttl`

## Requirements

Required host tools:

- Docker with Docker Compose v2
- `uv`
- Python 3.13 support through `uv`

Expected free host ports with default Compose settings:

- `8080`: read-balanced SPARQL endpoint through Nginx
- `8081`: write gateway (the only write entry point)
- `8089`: Locust UI

These can be changed with Compose environment variables such as `NGINX_HOST_PORT`, `LOCUST_HOST_PORT`, and `GATEWAY_HOST_PORT`.

## Configuration Overrides

The Compose file has defaults for local demo use. Override these only when needed:

- `RDF_DELTA_REF`, default `0a44c60368c523361fd2ba1d929023e5f1987ee0` — pinned commit of `afs/rdf-delta` built from source in `Dockerfile.fuseki` and `Dockerfile.delta-server`. See the README's "Why RDF Delta Instead Of Kafka" section before changing this; the pin exists because Maven Central's RDF Delta release predates Jena 6 and the upstream project may be archived.
- `JENA_VERSION` no longer applies to the Fuseki image (it now runs `delta-fuseki.jar`, built from the pinned RDF Delta source, not a downloaded Apache Fuseki distribution).
- `FUSEKI_MEM_LIMIT`, default `2g`
- `DELTA_SERVER_MEM_LIMIT`, default `512m`
- `DELTA_PATCHLOG`, default `rdf-events` — name of the RDF Delta patch log created by `delta-init`.
- `NGINX_HOST_PORT`, default `8080`
- `GATEWAY_HOST_PORT`, default `8081`
- `LOCUST_HOST_PORT`, default `8089`

For normal validation, use the defaults. If overriding ports, update any host-side command URLs accordingly.

## Setup

Install Python dependencies:

```bash
uv sync
```

Build the custom images:

```bash
docker compose build delta-server fuseki-writer write-gateway fuseki-reader locust
```

The first build compiles RDF Delta from source (a Maven multi-module build inside the Docker build stage) — expect this to take a few minutes. Subsequent builds reuse the BuildKit cache mount for `/root/.m2` and are fast.

Start the stack with two Fuseki reader replicas:

```bash
docker compose up -d --scale fuseki-reader=2
```

Check service status:

```bash
docker compose ps
```

Expected state:

- `delta-server` is healthy
- `delta-init` has exited successfully (it is a one-shot job, not a long-running service)
- `fuseki-writer` is healthy
- `fuseki-reader` replicas are healthy
- `write-gateway` is healthy
- `nginx-read` is healthy
- `locust` is running

## Important Endpoints

- Write gateway (the only write entry point): `http://localhost:8081/write`
- Read-balanced SPARQL endpoint: `http://localhost:8080/ds/sparql`
- Locust UI: `http://localhost:8089`

There is no host-exposed port for `delta-server` or `fuseki-writer` by default; reach them with `docker exec` if you need to query them directly (see Troubleshooting).

## Smoke Test

Produce a small amount of RDF data through the write gateway:

```bash
uv run jena-demo-produce --gateway-url http://localhost:8081 --events 25 --rate 25
```

Verify the replicated request count through the read-balanced SPARQL endpoint:

```bash
uv run jena-demo-stats --once
```

Expected output after replication catches up:

```text
requests=25
```

Replication is close to immediate (readers sync on each request), but under load it can lag briefly. If the count is lower than expected, wait briefly and run the stats command again.

## Load Test

Run a short headless mixed write/read load test:

```bash
uv run locust -f locustfile.py --headless --users 8 --spawn-rate 8 --run-time 20s -H http://localhost:8080
```

Expected result:

- Locust exits
- The summary includes `write rdf-event (gateway)` and SPARQL read requests
- A small failure rate (a few percent) on writes under this concurrency is expected and documented in the README — `fuseki-writer` is a single serial committer and every write round-trips to `delta-server`. Zero SPARQL read failures is expected; if reads fail, that is a real regression.

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

Unlike the earlier Kafka-based version of this demo, there is no consumer-group configuration to get right. Each reader has its own local `delta:zone` directory (never a shared volume); a fresh replica starts empty and syncs from `delta-server` on its first request. Do not mount a shared volume across `fuseki-reader` replicas for `/fuseki/zone` — RDF Delta explicitly forbids sharing a client zone between servers.

## Common Development Commands

Run the producer manually:

```bash
uv run jena-demo-produce --gateway-url http://localhost:8081 --events 1000 --rate 100
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

Reset all local compose state, including the RDF Delta patch store and the writer's TDB2 data:

```bash
docker compose down -v
```

Use `down -v` only when a clean run is needed, because it deletes the local patch log and the writer's zone data for this compose project — after that, `delta-init` will recreate the `rdf-events` log from scratch on the next `up`.

## Troubleshooting

If the write gateway reports commit failures:

```bash
docker compose ps delta-server fuseki-writer
docker compose logs fuseki-writer
docker compose logs delta-server
```

Query a Fuseki instance directly if you need to bypass the gateway/Nginx (replace the container name as needed):

```bash
docker exec <container> curl -s -X POST http://127.0.0.1:3030/ds/sparql \
  -H "Accept: application/sparql-results+json" \
  --data-urlencode 'query=PREFIX demo: <https://example.org/jena-demo#> SELECT (COUNT(?r) AS ?c) WHERE { GRAPH ?g { ?r a demo:Request } }'
```

List patch logs / inspect replication state on `delta-server`:

```bash
docker exec jena-demo-scale-delta-server-1 java -cp /delta/delta-server.jar dcmd ls --server http://127.0.0.1:1066/
```

If SPARQL reads fail:

```bash
docker compose ps nginx-read fuseki-reader
docker compose logs nginx-read
docker compose logs fuseki-reader
```

If request counts are lower than expected on a reader:

- wait a few seconds and rerun `uv run jena-demo-stats --once`
- compare against the writer's own count (see the `docker exec ... dcmd ls` / direct-query commands above) to confirm the writer itself has the data
- check `fuseki-reader` logs for sync errors against `delta-server`

If Docker image build fails while cloning/building RDF Delta from source, or downloading Python/Maven dependencies, it is usually a network or registry availability problem. Retry the build before changing project code. If it fails specifically inside the `mvn -q -DskipTests -pl rdf-delta-fuseki-server -am install` or `-pl rdf-delta-server` steps in `Dockerfile.fuseki` / `Dockerfile.delta-server`, that means the pinned `RDF_DELTA_REF` commit no longer builds against the Maven Central state at build time (e.g. a transitive dependency was pulled) — this is a real signal to investigate, not something to silently retry past.

## Agent Notes

- Prefer `rg` for searching files.
- Do not edit generated lock data unless dependency changes require it.
- Keep README architecture explanations intact unless the user explicitly asks to update them.
- For behavior changes, update or add focused verification commands in this file when useful.
- RDF Delta is built from source and pinned to a specific commit (`RDF_DELTA_REF`) rather than tracking a branch or a Maven Central release — see the README before changing this default, since it documents a real maintenance-risk trade-off that was deliberately investigated, not an arbitrary choice.
