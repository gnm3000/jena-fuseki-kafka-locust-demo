# Jena TDB2 + Kafka Horizontal Read Scaling Demo

This project demonstrates a realistic Apache Jena Fuseki + TDB2 scaling pattern using Kafka as the write-ahead event log and Locust as the load generator.

The key architectural point is that **TDB2 is not a distributed database**. A TDB2 dataset directory must be owned by one Fuseki process. Horizontal scaling is achieved by running multiple independent Fuseki reader replicas, each with its own local TDB2 store, and replaying the same RDF event stream from Kafka into every replica.

## Architecture

```text
Python / Locust writers
        |
        v
Apache Kafka topic: rdf-events
        |
        | each Fuseki reader uses a unique Kafka consumer group
        v
+----------------+   +----------------+   +----------------+   +----------------+
| Fuseki reader  |   | Fuseki reader  |   | Fuseki reader  |   | Fuseki reader  |
| local TDB2     |   | local TDB2     |   | local TDB2     |   | local TDB2     |
+----------------+   +----------------+   +----------------+   +----------------+
        ^                    ^                    ^                    ^
        |                    |                    |                    |
        +--------------------+--------- Nginx read load balancer -------+
                                      |
                                      v
                         SPARQL reads on localhost:8080
```

## Services

- `kafka`: official `apache/kafka:4.3.1`, KRaft single-node mode, JVM heap capped at 256 MiB, container memory capped at 450 MiB.
- `kafka-init`: creates `rdf-events` and `rdf-events.dlq` explicitly.
- `fuseki-reader`: scalable Fuseki 6.2.0 + TDB2 + Telicent Jena Fuseki Kafka module 3.1.0.
- `nginx-read`: load-balances read-only SPARQL traffic across the Fuseki readers.
- `locust`: custom `uv`-built image with the Python load test code and Kafka client.
- `kafka-ui`: Kafka topic/consumer inspection UI.

## Why Kafka Is Configured This Way

The demo now uses the standard official Kafka image instead of `apache/kafka-native`, but keeps resource usage low:

- `KAFKA_HEAP_OPTS=-Xms256m -Xmx256m` caps the JVM heap.
- `mem_limit: 450m` prevents the Kafka container from growing without bound.
- KRaft mode avoids ZooKeeper entirely.
- internal replication factors are set to `1`, which is appropriate for this single-node demo.
- `KAFKA_NUM_PARTITIONS=1` preserves event ordering, which matters if RDF Patch/delete semantics are introduced later.

For real production, use a multi-broker Kafka cluster or a managed Kafka service with replication, TLS/SASL, monitoring, backups, and topic retention sized for replay/recovery requirements.

## Start The Demo

```bash
uv sync
docker compose build fuseki-reader locust
docker compose up -d --scale fuseki-reader=2
```

Endpoints:

- Read-balanced SPARQL endpoint: `http://localhost:8080/ds/sparql`
- Locust UI: `http://localhost:8089`
- Kafka UI: `http://localhost:8090`
- Kafka bootstrap from host: `localhost:9092`
- Kafka bootstrap from containers: `kafka:29092`

## Produce Data Manually

```bash
uv run jena-demo-produce --events 10000 --rate 500
uv run jena-demo-stats --once
```

`jena-demo-produce` writes RDF N-Quads events to Kafka with `Content-Type: application/n-quads`. The Fuseki Kafka module consumes those events and applies them to each reader's local TDB2 dataset.

## Run Load Tests

Interactive UI:

```bash
open http://localhost:8089
```

Headless example:

```bash
uv run locust -f locustfile.py --headless --users 8 --spawn-rate 8 --run-time 20s -H http://localhost:8080
```

The Locust test has two user types:

- `WriterUser`: produces RDF events to Kafka.
- `ReaderUser`: sends SPARQL read queries through Nginx.

## Scaling Readers

Scale from 2 readers to 4 readers:

```bash
docker compose up -d --scale fuseki-reader=4
```

Every reader gets a unique Kafka `groupId`, derived from its container hostname. This is required. If all replicas share the same consumer group, Kafka partitions are divided between replicas and the replicas do **not** each receive the full dataset. With unique groups, every reader independently replays the full topic and converges to the same TDB2 state.

## Test Results

Environment date: 2026-08-21.

### Smoke Test

Initial smoke test with 2 Fuseki readers:

- Produced 25 RDF events with `uv run jena-demo-produce --events 25 --rate 25`.
- SPARQL count returned `requests=25` through `http://localhost:8080/ds/sparql`.
- Both readers consumed the topic completely with unique consumer groups.
- Kafka memory after smoke test: about `293.4 MiB / 450 MiB`.

### Short Mixed Load Test

Command:

```bash
uv run locust -f locustfile.py --headless --users 4 --spawn-rate 4 --run-time 10s -H http://localhost:8080
```

Result:

- Total operations: `533`.
- Failures: `0`.
- Kafka writes: `321` RDF events.
- SPARQL count queries: `162`.
- SPARQL group-by queries: `50`.
- Aggregate throughput: `54.74 req/s`.
- SPARQL count median latency: `12 ms`.
- SPARQL group-by median latency: `14 ms`.
- Kafka produce median latency: `0 ms` as measured by Locust client-side enqueue time.

### Scaled Reader Test

The reader tier was scaled from 2 to 4 replicas:

```bash
docker compose up -d --scale fuseki-reader=4
```

All 4 readers became healthy and each replayed the Kafka topic independently.

Command:

```bash
uv run locust -f locustfile.py --headless --users 8 --spawn-rate 8 --run-time 20s -H http://localhost:8080
```

Result:

- Fuseki readers: `4` healthy replicas.
- Total operations: `2,541`.
- Failures: `0`.
- Kafka writes: `1,899` RDF events.
- SPARQL count queries: `474`.
- SPARQL group-by queries: `168`.
- Aggregate throughput: `128.76 req/s`.
- Kafka write throughput: `96.23 events/s`.
- SPARQL read throughput: about `32.53 reads/s`.
- SPARQL count median latency: `14 ms`.
- SPARQL group-by median latency: `23 ms`.
- Final replicated dataset count: `requests=2245`.
- All 4 readers reached Kafka offset `rdf-events-0=2245` and reported `Completely up to date with Kafka topic(s)`.
- Kafka memory after scaled test: about `337.9 MiB / 450 MiB`.

## Did It Scale Correctly?

Yes for read-side horizontal scaling. The demo successfully scaled the Fuseki reader tier from 2 to 4 replicas while keeping Kafka as the single ordered event log. Each reader maintained its own TDB2 database and independently replayed the same Kafka topic. Nginx continued serving reads through one stable endpoint while Docker Compose added replicas.

This pattern is ready to scale the **read path** by adding Fuseki readers. It does not make TDB2 itself distributed, and it does not provide multi-writer TDB2 semantics. Writes should enter through Kafka as RDF events or RDF Patch events, and readers should project those events into local TDB2 stores.

## Production Readiness Notes

This demo is production-shaped, but not a complete production deployment.

What is realistic here:

- TDB2 is never shared between containers.
- Kafka is the replayable source of truth for RDF ingestion.
- Each reader uses a unique consumer group.
- Readers can be scaled horizontally.
- Read traffic is load-balanced through Nginx.
- Kafka has explicit topics and a DLQ topic.
- Fuseki runs as a non-root user in the custom image.
- Dockerfile builds use BuildKit cache mounts for Maven, APT, Fuseki downloads, and `uv`.

What must be added for production:

- multi-broker Kafka or managed Kafka, not a single broker;
- TLS/SASL and ACLs for Kafka;
- authentication/authorization in front of SPARQL endpoints;
- persistent storage strategy for every Fuseki reader;
- topic retention sized to allow full replay for new replicas;
- monitoring for consumer lag, DLQ volume, query latency, JVM memory, and disk growth;
- backup/restore strategy for Kafka and optional TDB2 snapshots;
- separate write and read network paths;
- stricter query timeouts and result limits for untrusted clients.
