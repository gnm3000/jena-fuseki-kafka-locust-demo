# Jena TDB2 Primary/Replica Scaling Demo (Kafka-Replicated)

This project demonstrates a realistic Apache Jena Fuseki + TDB2 scaling pattern using **the primary's TDB2 dataset as the source of truth**, Kafka as the durable replication transport to read replicas, and Locust as the load generator.

The key architectural point is that **TDB2 is not a distributed database**: it has no built-in primary/replica streaming replication the way Postgres or MySQL do. This demo gets primary/replica semantics anyway by putting a single authoritative Fuseki+TDB2 **writer** in front of all writes, and using Kafka purely as the transport that ships the writer's committed changes out to independent **reader** replicas, each with its own local TDB2 store.

## Architecture

```text
Locust / Python writers
        |
        v
+-------------------+     synchronous commit      +--------------------+
|   write-gateway    | ---------------------------> |   fuseki-writer    |
| (commit gate)      | <--- 200 OK only on commit -- | TDB2 (source of    |
+-------------------+                               | truth)             |
        |                                            +--------------------+
        | only published after the writer commits
        v
Apache Kafka topic: rdf-events   (replication transport, not the source of truth)
        |
        | each Fuseki reader uses a unique Kafka consumer group
        v
+----------------+   +----------------+   +----------------+   +----------------+
| Fuseki reader  |   | Fuseki reader  |   | Fuseki reader  |   | Fuseki reader  |
| local TDB2     |   | local TDB2     |   | local TDB2     |   | local TDB2     |
| (follower)     |   | (follower)     |   | (follower)     |   | (follower)     |
+----------------+   +----------------+   +----------------+   +----------------+
        ^                    ^                    ^                    ^
        |                    |                    |                    |
        +--------------------+--------- Nginx read load balancer -------+
                                      |
                                      v
                         SPARQL reads on localhost:8080
```

### Write path: the DB is the durability gate

The `write-gateway` is the only write entry point. For every incoming event it:

1. Converts the N-Quads payload into a SPARQL `INSERT DATA` and sends it to `fuseki-writer`'s `/ds/update` endpoint, **synchronously**, and waits for the commit to succeed.
2. Only if that commit succeeds does it publish the same N-Quads to the `rdf-events` Kafka topic.
3. If the Kafka publish fails after the DB commit, it returns an error to the caller rather than silently swallowing the gap. Because RDF `INSERT DATA` is naturally idempotent (inserting the same triple twice is a no-op), the caller can safely retry the same event without risk of duplication on the primary.

This means `fuseki-writer`'s TDB2 is authoritative: if the gateway returns success, the write is durably committed in the primary, independent of whether Kafka or any reader has seen it yet. Kafka is downstream of that commit — it exists only because TDB2 itself cannot stream its own write-ahead log to followers, so this demo builds that shipping mechanism explicitly instead.

The trade-off versus the previous Kafka-as-source-of-truth design is latency: every write now pays for a synchronous round trip to the primary's TDB2 commit *and* a Kafka publish before it is acknowledged, instead of a fire-and-forget produce. That is the expected cost of moving durability from the log to the database.

## Services

- `fuseki-writer`: single Fuseki 6.2.0 + TDB2 instance, the **primary**/source of truth. Accepts SPARQL Update/GSP directly; has no Kafka connector.
- `write-gateway`: Flask/Waitress HTTP service and the only write entry point. Commits each write to `fuseki-writer` synchronously, then republishes it to Kafka for the replicas.
- `kafka`: official `apache/kafka:4.3.1`, KRaft single-node mode, JVM heap capped at 256 MiB, container memory capped at 450 MiB. Used here purely as the writer's replication transport.
- `kafka-init`: creates `rdf-events` and `rdf-events.dlq` explicitly.
- `fuseki-reader`: scalable Fuseki 6.2.0 + TDB2 + Telicent Jena Fuseki Kafka module 3.1.0. Each replica is a read-only **follower** that replays `rdf-events` into its own local TDB2.
- `nginx-read`: load-balances read-only SPARQL traffic across the Fuseki readers.
- `locust`: custom `uv`-built image with the Python load test code; `WriterUser` posts to `write-gateway`, `ReaderUser` queries through `nginx-read`.
- `kafka-ui`: Kafka topic/consumer inspection UI.

## RDF And Ontology Model

This demo uses RDF because the data is naturally represented as a graph: a request belongs to a tenant, is served by a service, has a timestamp, has a status code, and has a measured latency. RDF stores those facts as triples:

```text
subject              predicate              object
demo:request/abc123  rdf:type               demo:Request
demo:request/abc123  demo:servedBy          demo:checkout
demo:request/abc123  demo:belongsToTenant   demo:tenant-a
demo:request/abc123  demo:createdAt         "2026-08-21T22:49:32.123Z"^^xsd:dateTime
demo:request/abc123  demo:statusCode        "200"^^xsd:integer
demo:request/abc123  demo:latencyMs         "41.7"^^xsd:decimal
```

Kafka messages are encoded as **N-Quads**, not plain Turtle. N-Quads adds a fourth term: the graph name. This demo writes all generated operational data into the named graph `https://example.org/jena-demo#graph/load`. That is why the SPARQL queries use `GRAPH ?g { ... }` instead of reading only the default graph.

### Ontology

The ontology lives in `ontology/demo.ttl`. It defines a small domain model:

- `demo:Request`: one observed application/API request.
- `demo:Service`: an application service that handled a request.
- `demo:Tenant`: a customer/account namespace that owns the request.
- `demo:servedBy`: links a request to the service that handled it.
- `demo:belongsToTenant`: links a request to the tenant.
- `demo:createdAt`: timestamp for the request.
- `demo:statusCode`: simulated HTTP status code.
- `demo:latencyMs`: simulated request latency.

The ontology file also includes a few seed individuals, such as `demo:checkout`, `demo:catalog`, and `demo:tenant-a`, mainly to make the model concrete. The load generator can emit more service and tenant IRIs than those seed examples. Each generated event includes type triples for the selected service and tenant, so the graph remains self-describing even when `pricing`, `search`, `tenant-b`, or `tenant-c` appear.

### What The Writer Simulates

Each Python/Locust write simulates one request event from an operational system. The generator randomly chooses:

- one service from `checkout`, `catalog`, `pricing`, `search`;
- one tenant from `tenant-a`, `tenant-b`, `tenant-c`;
- one status code, usually `200`, with occasional `429`, `500`, or `503`;
- one latency value between roughly `10 ms` and `260 ms`;
- one timestamp at write time.

For every simulated request, the writer emits about 8 RDF statements:

- the request is a `demo:Request`;
- the request was served by one service;
- the request belongs to one tenant;
- the request has `createdAt`, `statusCode`, and `latencyMs` literals;
- the selected service is typed as `demo:Service`;
- the selected tenant is typed as `demo:Tenant`;
- about 1% of events also get `demo:sampled true`.

So after `N` generated request events, the dataset should contain approximately `N` request nodes plus a small bounded set of reusable service and tenant nodes. In the measured scaled test, the final count was `2,245` request nodes, and each of the 4 Fuseki readers replayed Kafka up to offset `2,245`.

### What Nodes Should Exist

After a small run, the graph should contain these kinds of RDF resources:

- request nodes: many unique IRIs like `demo:request/<uuid>`, one per generated event;
- service nodes: up to 4 stable IRIs, `demo:checkout`, `demo:catalog`, `demo:pricing`, `demo:search`;
- tenant nodes: up to 3 stable IRIs, `demo:tenant-a`, `demo:tenant-b`, `demo:tenant-c`;
- the named graph node: `demo:graph/load`, used as the N-Quads graph target.

The important scaling behavior is that every reader should converge to the same request count as `fuseki-writer`, because every reader consumes the full Kafka topic through its own consumer group. If one reader has fewer request nodes than the writer, it is behind Kafka replication or was misconfigured to share a consumer group. The writer's own count, queried directly, is the ground truth to compare replicas against.

### What The Read Load Simulates

The Locust read users simulate dashboard/reporting traffic over SPARQL:

```sparql
PREFIX demo: <https://example.org/jena-demo#>
SELECT (COUNT(?request) AS ?requests)
WHERE { GRAPH ?g { ?request a demo:Request . } }
```

This query answers: "how many request events have been replicated into this reader?" It is the main correctness check.

```sparql
PREFIX demo: <https://example.org/jena-demo#>
SELECT ?service (COUNT(?request) AS ?requests)
WHERE {
  GRAPH ?g {
    ?request a demo:Request ;
             demo:servedBy ?service .
  }
}
GROUP BY ?service
ORDER BY DESC(?requests)
LIMIT 10
```

This query answers: "which services are receiving the most requests?" It is a simple analytical query that exercises grouping and aggregation over the RDF graph.

The write load tests the write-gateway's synchronous commit-to-primary path and TDB2 projection into replicas. The read load tests Nginx distribution and Fuseki/TDB2 query performance. Together they show the intended pattern: writes commit to the primary once, Kafka ships that commit to every reader, and reads scale horizontally by adding more Fuseki replicas.

## Why TDB2 Needs A Primary And Kafka Needs To Exist At All

TDB2 has no native primary/replica replication: no WAL shipping, no streaming replication, no cluster mode. If a single TDB2 instance is going to be the source of truth, something still has to carry its committed changes out to followers — that's what Kafka is doing here, playing the role a database's own replication log would play if TDB2 had one.

The demo uses the standard official Kafka image instead of `apache/kafka-native`, but keeps resource usage low:

- `KAFKA_HEAP_OPTS=-Xms256m -Xmx256m` caps the JVM heap.
- `mem_limit: 450m` prevents the Kafka container from growing without bound.
- KRaft mode avoids ZooKeeper entirely.
- internal replication factors are set to `1`, which is appropriate for this single-node demo.
- `KAFKA_NUM_PARTITIONS=1` preserves event ordering, which matters since the writer is a single serial commit stream and readers must apply patches in the same order.

For real production, use a multi-broker Kafka cluster or a managed Kafka service with replication, TLS/SASL, monitoring, backups, and topic retention sized for replay/recovery requirements. The primary (`fuseki-writer`) is also a single point of failure in this demo — production would need its own failover story (standby TDB2 + promotion, or a managed/clustered triplestore), which is out of scope here.

## Start The Demo

```bash
uv sync
docker compose build fuseki-writer write-gateway fuseki-reader locust
docker compose up -d --scale fuseki-reader=2
```

Endpoints:

- Write gateway (the only write entry point): `http://localhost:8081/write`
- Read-balanced SPARQL endpoint: `http://localhost:8080/ds/sparql`
- Locust UI: `http://localhost:8089`
- Kafka UI: `http://localhost:8090`
- Kafka bootstrap from host: `localhost:9092`
- Kafka bootstrap from containers: `kafka:29092`

## Produce Data Manually

```bash
uv run jena-demo-produce --gateway-url http://localhost:8081 --events 10000 --rate 500
uv run jena-demo-stats --once
```

`jena-demo-produce` posts RDF N-Quads events to the write gateway with `Content-Type: application/n-quads`. The gateway commits each event to `fuseki-writer`'s TDB2 synchronously, then republishes it to Kafka; the Fuseki Kafka module on each reader consumes that topic and applies the same changes to its own local TDB2 dataset.

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

- `WriterUser`: posts RDF events to the write gateway (absolute URL, independent of `-H`/`--host`, which only applies to `ReaderUser`).
- `ReaderUser`: sends SPARQL read queries through Nginx.

## Scaling Readers

Scale from 2 readers to 4 readers:

```bash
docker compose up -d --scale fuseki-reader=4
```

Every reader gets a unique Kafka `groupId`, derived from its container hostname. This is required. If all replicas share the same consumer group, Kafka partitions are divided between replicas and the replicas do **not** each receive the full dataset. With unique groups, every reader independently replays the full topic and converges to the same TDB2 state.

## Test Results

Environment date: 2026-08-21. **These numbers were measured under the previous architecture, where Locust/`jena-demo-produce` wrote directly to Kafka and no `fuseki-writer`/`write-gateway` existed.** They are kept for historical reference on Kafka/reader throughput; they do not reflect the added write-gateway commit latency described above. Re-run the load test against the current stack for up-to-date numbers.

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

Yes for read-side horizontal scaling. The demo scales the Fuseki reader tier from 2 to 4 replicas while `fuseki-writer` remains the single authoritative primary. Each reader maintains its own TDB2 database and independently replays the same Kafka replication topic. Nginx continues serving reads through one stable endpoint while Docker Compose adds replicas.

This pattern scales the **read path** by adding Fuseki readers. It does not make TDB2 itself distributed, and the write path is intentionally single-primary: all writes commit through one `fuseki-writer` instance, which is the guarantee this design trades for (a real source-of-truth DB), at the cost of the primary being a scaling and availability bottleneck for writes.

## Production Readiness Notes

This demo is production-shaped, but not a complete production deployment.

What is realistic here:

- `fuseki-writer`'s TDB2 is the single source of truth; the write-gateway will not acknowledge a write until it is durably committed there.
- TDB2 is never shared between containers.
- Kafka is the replayable replication transport that ships the primary's committed changes to followers, not the source of truth itself.
- Each reader uses a unique consumer group.
- Readers can be scaled horizontally.
- Read traffic is load-balanced through Nginx.
- Kafka has explicit topics and a DLQ topic.
- Fuseki runs as a non-root user in the custom image.
- Dockerfile builds use BuildKit cache mounts for Maven, APT, Fuseki downloads, and `uv`.

What must be added for production:

- failover for `fuseki-writer` itself: it is a single point of failure for writes in this demo (standby TDB2 + promotion, or a managed/clustered triplestore);
- true durability across the commit-then-publish gap: the writer commit and the Kafka publish are two separate steps, not one atomic transaction (a transactional outbox or WAL-tailing approach would close this gap; this demo accepts it and relies on retry + RDF's insert idempotency instead);
- multi-broker Kafka or managed Kafka, not a single broker;
- TLS/SASL and ACLs for Kafka;
- authentication/authorization in front of the write gateway and SPARQL endpoints;
- persistent storage strategy for every Fuseki reader and for the writer;
- topic retention sized to allow full replay for new replicas;
- monitoring for consumer lag, DLQ volume, query latency, JVM memory, and disk growth;
- backup/restore strategy for Kafka and TDB2 snapshots (writer and readers);
- separate write and read network paths;
- stricter query timeouts and result limits for untrusted clients.
