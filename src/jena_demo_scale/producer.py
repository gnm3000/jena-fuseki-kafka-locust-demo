from __future__ import annotations

import argparse
import os
import time

import requests

from .rdf import event_nquads


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Produce RDF N-Quads write events through the write gateway, "
        "which commits them to the writer's TDB2 before RDF Delta replicates them to readers."
    )
    parser.add_argument("--gateway-url", default=os.getenv("GATEWAY_URL", "http://localhost:8081"))
    parser.add_argument("--events", type=int, default=1000)
    parser.add_argument("--rate", type=float, default=100.0, help="Target events per second. Use 0 for max throughput.")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    delay = 0.0 if args.rate <= 0 else 1.0 / args.rate
    delivered = 0
    started = time.perf_counter()
    errors: list[str] = []

    session = requests.Session()
    for _index in range(args.events):
        response = session.post(
            f"{args.gateway_url}/write",
            data=event_nquads(),
            headers={"Content-Type": "application/n-quads"},
            timeout=15,
        )
        if response.status_code >= 300:
            errors.append(f"{response.status_code}: {response.text[:200]}")
        else:
            delivered += 1
        if delay:
            time.sleep(delay)
        if delivered % 1000 == 0 and delivered:
            elapsed = max(time.perf_counter() - started, 0.001)
            print(f"produced={delivered} rate={delivered / elapsed:.1f}/s")

    elapsed = max(time.perf_counter() - started, 0.001)
    print(f"done produced={delivered} elapsed={elapsed:.2f}s avg_rate={delivered / elapsed:.1f}/s")
    if errors:
        raise RuntimeError(f"write gateway failures: {errors[:3]}")
