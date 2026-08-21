from __future__ import annotations

import argparse
import os
import time

from confluent_kafka import Producer

from .rdf import event_nquads


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Produce RDF N-Quads events to Kafka for the Jena demo.")
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "rdf-events"))
    parser.add_argument("--events", type=int, default=1000)
    parser.add_argument("--rate", type=float, default=100.0, help="Target events per second. Use 0 for max throughput.")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    producer = Producer(
        {
            "bootstrap.servers": args.bootstrap_servers,
            "client.id": "jena-demo-producer",
            "linger.ms": 20,
            "batch.num.messages": 1000,
            "compression.type": "zstd",
            "acks": "all",
        }
    )
    delay = 0.0 if args.rate <= 0 else 1.0 / args.rate
    delivered = 0
    started = time.perf_counter()
    errors: list[str] = []

    def on_delivery(err, _msg) -> None:
        if err is not None:
            errors.append(str(err))

    for _index in range(args.events):
        producer.produce(
            args.topic,
            value=event_nquads(),
            headers={"Content-Type": "application/n-quads"},
            on_delivery=on_delivery,
        )
        delivered += 1
        producer.poll(0)
        if delay:
            time.sleep(delay)
        if delivered % 1000 == 0:
            elapsed = max(time.perf_counter() - started, 0.001)
            print(f"produced={delivered} rate={delivered / elapsed:.1f}/s")

    producer.flush()
    if errors:
        raise RuntimeError(f"Kafka delivery failed: {errors[:3]}")
    elapsed = max(time.perf_counter() - started, 0.001)
    print(f"done produced={delivered} elapsed={elapsed:.2f}s avg_rate={delivered / elapsed:.1f}/s")
