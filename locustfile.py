from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "src"))

from locust import HttpUser, between, task

from jena_demo_scale.rdf import event_nquads

WRITE_BASE_URL = os.getenv("WRITE_BASE_URL", "http://write-gateway:8081")
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


class WriterUser(HttpUser):
    # Locust's --host flag overwrites User.host for every User class, so an
    # absolute URL is used here to keep writes pinned to the write-gateway
    # even though --host points ReaderUser at the read-balanced Nginx endpoint.
    host = WRITE_BASE_URL
    wait_time = between(0.01, 0.05)
    weight = 4

    @task
    def write_rdf_event(self) -> None:
        self.client.post(
            f"{WRITE_BASE_URL}/write",
            data=event_nquads(),
            headers={"Content-Type": "application/n-quads"},
            name="write rdf-event (gateway)",
            timeout=15,
        )


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
