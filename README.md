# Jena TDB2 Primary/Replica Scaling Demo (RDF Delta Replicated)

This project demonstrates a realistic Apache Jena Fuseki + TDB2 scaling pattern using **the primary's TDB2 dataset as the source of truth**, [RDF Delta](https://afs.github.io/rdf-delta) as the durable replication transport to read replicas, and Locust as the load generator.

The key architectural point is that **TDB2 is not a distributed database**: it has no built-in primary/replica streaming replication the way Postgres or MySQL do. This demo gets primary/replica semantics anyway by putting a single authoritative Fuseki+TDB2 **writer** in front of all writes, and using an RDF Delta Patch Log Server as the transport that ships the writer's committed changes out to independent **reader** replicas, each with its own local TDB2 store.

An earlier version of this demo used Kafka for that transport instead. RDF Delta replaces it here because it is purpose-built for exactly this job (patch-log shipping between Fuseki instances) and needs far less infrastructure — no broker cluster, no ZooKeeper, no partitions or consumer groups. See [Why RDF Delta Instead Of Kafka](#why-rdf-delta-instead-of-kafka) for the trade-offs and a real caveat about that project's maintenance status.

## Architecture

```text
Locust / Python writers
        |
        v
+-------------------+     synchronous commit      +--------------------+
|   write-gateway    | ---------------------------> |   fuseki-writer    |
| (commit gate)      | <--- 200 OK only when the --- | TDB2 + RDF Delta   |
+-------------------+      patch is durable in       | client (source of  |
                           the patch log              | truth)             |
                                                       +--------------------+
                                                                |
                                                                | patch committed
                                                                v
                                              RDF Delta Patch Log Server (delta-server)
                                                     patch log: "rdf-events"
                                                                |
                                                                | each reader syncs its own
                                                                | local zone on every request
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

### Write path: the DB (and its replication log) is the durability gate

`fuseki-writer`'s dataset is an RDF Delta `delta:DeltaDataset`, not a plain `tdb2:DatasetTDB2`. That changes what "committed" means: Fuseki does **not** consider a SPARQL Update transaction committed until the corresponding RDF Patch has been durably recorded in the RDF Delta patch log. The local TDB2 write and the patch-log append happen as one logical unit inside Fuseki itself — the `write-gateway` doesn't have to orchestrate that two-step process by hand the way the earlier Kafka-based version did.

So the `write-gateway`'s job is now just:

1. Convert the incoming N-Quads into a SPARQL `INSERT DATA` update.
2. POST it to `fuseki-writer`'s `/ds/update` endpoint and wait.
3. Return whatever `fuseki-writer` returns: a 200 means the write is durable in **both** the primary's TDB2 and the replication log; anything else means it isn't durable anywhere.

This closes a gap that existed in the Kafka version of this demo: there, the writer's TDB2 commit and the Kafka publish were two separate steps the gateway had to sequence itself, with a small window where the primary had the data but the log didn't yet. With RDF Delta, Fuseki treats "commit to TDB2" and "commit to the replication log" as a single unit — there's no separate step where they can disagree, because there's no separate step at all.

The reader replicas run Fuseki with the **same** `delta:DeltaDataset` type, just pointed at a config with no update endpoint exposed (see `fuseki/config-reader.ttl`). On every request that needs the latest data, a reader checks the patch log for its current version and catches up before answering. That is why a brand-new reader replica, starting from an empty local zone, converges to the writer's full dataset without any Kafka-style "consumer group" or offset bookkeeping to configure.

## Services

- `fuseki-writer`: single Fuseki instance built on `delta-fuseki.jar`, the **primary**/source of truth. Only this instance exposes a SPARQL Update endpoint.
- `write-gateway`: Flask/Waitress HTTP service and the only write entry point. Forwards each write to `fuseki-writer` synchronously and reports back exactly what the writer reports.
- `delta-server`: the RDF Delta Patch Log Server (`delta-server.jar`), running in its simplest supported mode — a single process with plain-file patch storage (`--base`), no ZooKeeper, no S3. This is the transport; see below for why this mode specifically was chosen.
- `delta-init`: one-shot container that creates the `rdf-events` patch log on `delta-server` if it doesn't already exist yet (idempotent, so `docker compose up` can be re-run safely).
- `fuseki-reader`: scalable Fuseki instance, same `delta-fuseki.jar` image as the writer but a read-only config. Each replica is a **follower** that syncs from the `rdf-events` patch log into its own local TDB2-backed zone.
- `nginx-read`: load-balances read-only SPARQL traffic across the Fuseki readers.
- `locust`: custom `uv`-built image with the Python load test code; `WriterUser` posts to `write-gateway`, `ReaderUser` queries through `nginx-read`.

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

Writes are sent as **N-Quads**, not plain Turtle. N-Quads adds a fourth term: the graph name. This demo writes all generated operational data into the named graph `https://example.org/jena-demo#graph/load`. That is why the SPARQL queries use `GRAPH ?g { ... }` instead of reading only the default graph.

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

So after `N` generated request events, the dataset should contain approximately `N` request nodes plus a small bounded set of reusable service and tenant nodes.

### What Nodes Should Exist

After a small run, the graph should contain these kinds of RDF resources:

- request nodes: many unique IRIs like `demo:request/<uuid>`, one per generated event;
- service nodes: up to 4 stable IRIs, `demo:checkout`, `demo:catalog`, `demo:pricing`, `demo:search`;
- tenant nodes: up to 3 stable IRIs, `demo:tenant-a`, `demo:tenant-b`, `demo:tenant-c`;
- the named graph node: `demo:graph/load`, used as the N-Quads graph target.

The important scaling behavior is that every reader should converge to the same request count as `fuseki-writer`, because every reader syncs from the same RDF Delta patch log. If one reader has fewer request nodes than the writer, it is behind on replication. The writer's own count, queried directly, is the ground truth to compare replicas against.

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

The write load tests the write-gateway's synchronous commit-to-primary path, which now also includes the primary's patch-log commit. The read load tests Nginx distribution and Fuseki/TDB2 query performance. Together they show the intended pattern: writes commit to the primary (and its replication log) once, and reads scale horizontally by adding more Fuseki replicas.

## Why RDF Delta Instead Of Kafka

TDB2 has no native primary/replica replication: no WAL shipping, no streaming replication, no cluster mode. Something still has to carry the primary's committed changes out to followers. [RDF Delta](https://github.com/afs/rdf-delta) is Apache Jena's own answer to that problem: an RDF Patch Log Server plus a Fuseki client module (`delta:DeltaDataset`), built by the same people who maintain Jena/TDB2/Fuseki.

**A real caveat, checked directly against the project, not assumed:** the last release of RDF Delta on Maven Central (`1.1.2`, 2022) predates Jena 6 and is not what this demo uses. The maintainer, Andy Seaborne, has also stated on the Jena mailing list that the separate `rdf-delta` project will eventually be archived — the ZooKeeper-backed HA mode and the S3 patch-storage backend (whose AWS SDK v1 dependency reached end-of-life) were too much to keep maintaining as a side project. He specifically said the **plain file-backed patch server** (no ZooKeeper, no S3) will keep being supported, because it's the low-maintenance mode. That is exactly the mode this demo uses (`delta-server --base DIR`). Because of the Maven Central gap, `Dockerfile.fuseki` and `Dockerfile.delta-server` build RDF Delta from source, pinned to a specific commit (`RDF_DELTA_REF`, currently `0a44c60368c523361fd2ba1d929023e5f1987ee0`) that the upstream repo's own history confirms was made to fix warnings against Jena 6.2.0 — i.e., a commit the maintainers themselves verified against the exact Jena version this demo runs.

Given that maintenance outlook, treat this integration as **validated to work today, not a dependency to lean on indefinitely**. A from-scratch replication plugin built directly on Jena's own `RDFPatch` format (which is not going away, since it's used by Jena core independent of the separate `rdf-delta` project) remains the lower-risk long-term option; see the git history/discussion for that alternative design. Because the build pins an exact commit rather than a moving branch, this demo won't silently break if `rdf-delta` is archived or changes later — it just won't get any further upstream fixes either.

## Start The Demo

```bash
uv sync
docker compose build delta-server fuseki-writer write-gateway fuseki-reader locust
docker compose up -d --scale fuseki-reader=2
```

The first build compiles RDF Delta from source (see above), which takes a few minutes; subsequent builds reuse the BuildKit Maven cache and are fast.

Endpoints:

- Write gateway (the only write entry point): `http://localhost:8081/write`
- Read-balanced SPARQL endpoint: `http://localhost:8080/ds/sparql`
- Locust UI: `http://localhost:8089`

## Produce Data Manually

```bash
uv run jena-demo-produce --gateway-url http://localhost:8081 --events 10000 --rate 500
uv run jena-demo-stats --once
```

`jena-demo-produce` posts RDF N-Quads events to the write gateway with `Content-Type: application/n-quads`. The gateway forwards each event to `fuseki-writer`'s update endpoint; the write is not acknowledged until it is durable in both the writer's TDB2 and the RDF Delta patch log. Readers pick up the change the next time they sync.

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

Under a burst of many concurrent `WriterUser`s, expect some write latency and occasional gateway timeouts: `fuseki-writer` is a single serial committer, and every commit now also has to round-trip to `delta-server`. That backpressure is expected — it is the cost of a real single-primary source of truth, not a bug. See [Did It Scale Correctly?](#did-it-scale-correctly).

## Scaling Readers

Scale from 2 readers to 4 readers:

```bash
docker compose up -d --scale fuseki-reader=4
```

There is no consumer-group configuration to get right here (unlike the earlier Kafka version). Each reader keeps its own local zone directory (`delta:zone`), which is never shared between replicas and never mounted to a host volume — a fresh reader starts with an empty zone and simply syncs from `delta-server` up to the current version on its first request. Verified directly: scaling from 2 to 4 readers mid-run, the two new replicas converged to the same request count as the writer and the existing readers with no extra configuration.

## Did It Scale Correctly?

Yes for read-side horizontal scaling, verified end-to-end: writes through `write-gateway` land on `fuseki-writer`, and both existing and newly-scaled `fuseki-reader` replicas converge to the same count as the writer. This pattern scales the **read path** by adding Fuseki readers. It does not make TDB2 itself distributed, and the write path is intentionally single-primary: all writes commit through one `fuseki-writer` instance, which is the guarantee this design trades for (a real source-of-truth DB), at the cost of the primary being a scaling and availability bottleneck for writes.

Under concurrent load testing (8 Locust users, low wait time), a small fraction of writes hit gateway-side timeouts while the writer and patch server catch up — observed directly, not estimated. This is a real, documented characteristic of RDF Delta's single patch-server design (the upstream project has an open issue about the patch server struggling under heavy concurrent sync traffic), not something specific to this demo's code. It reinforces that this architecture buys consistency and a real source of truth at the cost of single-writer write throughput.

## Production Readiness Notes

This demo is production-shaped, but not a complete production deployment.

What is realistic here:

- `fuseki-writer`'s TDB2 + RDF Delta patch log together are the source of truth; a write is acknowledged only once both are durable.
- TDB2 is never shared between containers.
- The RDF Delta patch log is the replayable replication transport; new readers bootstrap from it with no manual offset/group configuration.
- Readers can be scaled horizontally with zero replication config beyond pointing at the same patch log.
- Read traffic is load-balanced through Nginx.
- Fuseki and the patch server both run as non-root users in their custom images.
- Dockerfile builds use BuildKit cache mounts for Maven and APT.

What must be added for production:

- failover for `fuseki-writer` itself: it is a single point of failure for writes in this demo (standby TDB2 + promotion, or a managed/clustered triplestore);
- failover for `delta-server` itself: this demo intentionally runs the single-process, no-ZooKeeper mode (the mode the RDF Delta maintainer says will keep being supported), which means the patch server is also a single point of failure — RDF Delta does support a ZooKeeper-backed HA mode for the patch log index, but per the maintainer's own account it is the harder-to-maintain, higher-risk option, so it was deliberately left out here;
- a decision on the RDF Delta maintenance risk described above before relying on this in a real system: either accept the pinned-commit build as-is, or replace it with a from-scratch plugin built on Jena's own `RDFPatch` format;
- authentication/authorization in front of the write gateway and SPARQL endpoints;
- persistent storage strategy for the patch server's store and for the writer's zone (both are on named Docker volumes here, which is a start, not a full backup story);
- monitoring for patch-log lag, sync latency, query latency, JVM memory, and disk growth;
- backup/restore strategy for the patch store and TDB2 snapshots (writer and readers);
- separate write and read network paths;
- stricter query timeouts and result limits for untrusted clients.
