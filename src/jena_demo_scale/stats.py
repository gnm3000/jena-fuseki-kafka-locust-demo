from __future__ import annotations

import argparse
import time

import requests

COUNT_QUERY = """
PREFIX demo: <https://revsavvy.ai/demo#>
SELECT (COUNT(?request) AS ?requests)
WHERE { GRAPH ?g { ?request a demo:Request . } }
"""


def sparql(endpoint: str, query: str) -> dict:
    response = requests.post(
        endpoint,
        data={"query": query},
        headers={"Accept": "application/sparql-results+json"},
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def count(endpoint: str) -> int:
    payload = sparql(endpoint, COUNT_QUERY)
    value = payload["results"]["bindings"][0]["requests"]["value"]
    return int(value)


def main() -> None:
    parser = argparse.ArgumentParser(description="Poll the read-balanced Fuseki endpoint and print dataset growth.")
    parser.add_argument("--endpoint", default="http://localhost:8080/ds/sparql")
    parser.add_argument("--interval", type=float, default=2.0)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()

    previous_count = None
    previous_time = None

    while True:
        now = time.perf_counter()
        current = count(args.endpoint)
        if previous_count is None:
            print(f"requests={current}")
        else:
            elapsed = max(now - previous_time, 0.001)
            print(f"requests={current} replicated_rate={(current - previous_count) / elapsed:.1f}/s")

        if args.once:
            break
        previous_count = current
        previous_time = now
        time.sleep(args.interval)
