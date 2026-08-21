from __future__ import annotations

import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "src"))

from confluent_kafka import Producer
from locust import HttpUser, between, events, task

from jena_demo_scale.rdf import event_nquads

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "rdf-events")
DATASET_PATH = os.getenv("DATASET_PATH", "/ds")

COUNT_QUERY = """
PREFIX demo: <https://example.org/jena-demo#>
SELECT (COUNT(?request) AS ?requests)
WHERE { GRAPH ?g { ?request a demo:Request . } }
"""

SERVICE_QUERY = """
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
"""


class KafkaProducerUser(HttpUser):
    abstract = True

    def on_start(self) -> None:
        self.producer = Producer(
            {
                "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
                "client.id": "locust-jena-demo",
                "linger.ms": 20,
                "batch.num.messages": 1000,
                "compression.type": "zstd",
                "acks": "all",
            }
        )

    def produce_event(self) -> None:
        started = time.perf_counter()
        try:
            self.producer.produce(
                KAFKA_TOPIC,
                value=event_nquads(),
                headers={"Content-Type": "application/n-quads"},
            )
            self.producer.poll(0)
            events.request.fire(
                request_type="KAFKA",
                name="produce rdf-events",
                response_time=(time.perf_counter() - started) * 1000,
                response_length=0,
                exception=None,
                context={},
            )
        except Exception as exc:
            events.request.fire(
                request_type="KAFKA",
                name="produce rdf-events",
                response_time=(time.perf_counter() - started) * 1000,
                response_length=0,
                exception=exc,
                context={},
            )

    def on_stop(self) -> None:
        self.producer.flush(5)


class WriterUser(KafkaProducerUser):
    wait_time = between(0.01, 0.05)
    weight = 4

    @task
    def write_rdf_event(self) -> None:
        self.produce_event()


class ReaderUser(HttpUser):
    wait_time = between(0.05, 0.2)
    weight = 8

    @task(3)
    def count_requests(self) -> None:
        self.client.post(
            f"{DATASET_PATH}/sparql",
            data={"query": COUNT_QUERY},
            headers={"Accept": "application/sparql-results+json"},
            name="SPARQL count requests",
            timeout=30,
        )

    @task(1)
    def group_by_service(self) -> None:
        self.client.post(
            f"{DATASET_PATH}/sparql",
            data={"query": SERVICE_QUERY},
            headers={"Accept": "application/sparql-results+json"},
            name="SPARQL group by service",
            timeout=30,
        )
