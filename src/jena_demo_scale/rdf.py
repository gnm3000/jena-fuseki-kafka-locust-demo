from __future__ import annotations

from datetime import datetime, timezone
from random import choice, randint, random
from uuid import uuid4

DEMO = "https://revsavvy.ai/demo#"
SERVICES = ("checkout", "catalog", "pricing", "search")
TENANTS = ("tenant-a", "tenant-b", "tenant-c")


def event_nquads() -> bytes:
    event_id = uuid4().hex
    now = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    service = choice(SERVICES)
    tenant = choice(TENANTS)
    status = 200 if random() < 0.97 else choice((429, 500, 503))
    latency = round(10 + random() * 250, 3)

    subject = f"<{DEMO}request/{event_id}>"
    graph = f"<{DEMO}graph/load>"
    lines = [
        f'{subject} <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <{DEMO}Request> {graph} .',
        f'{subject} <{DEMO}servedBy> <{DEMO}{service}> {graph} .',
        f'{subject} <{DEMO}belongsToTenant> <{DEMO}{tenant}> {graph} .',
        f'{subject} <{DEMO}createdAt> "{now}"^^<http://www.w3.org/2001/XMLSchema#dateTime> {graph} .',
        f'{subject} <{DEMO}statusCode> "{status}"^^<http://www.w3.org/2001/XMLSchema#integer> {graph} .',
        f'{subject} <{DEMO}latencyMs> "{latency}"^^<http://www.w3.org/2001/XMLSchema#decimal> {graph} .',
        f'<{DEMO}{service}> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <{DEMO}Service> {graph} .',
        f'<{DEMO}{tenant}> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <{DEMO}Tenant> {graph} .',
    ]

    if randint(1, 100) == 1:
        lines.append(f'{subject} <{DEMO}sampled> "true"^^<http://www.w3.org/2001/XMLSchema#boolean> {graph} .')

    return ("\n".join(lines) + "\n").encode("utf-8")
