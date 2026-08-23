from __future__ import annotations

import os

import requests
from flask import Flask, Response, request
from rdflib import ConjunctiveGraph

WRITER_BASE_URL = os.getenv("WRITER_BASE_URL", "http://fuseki-writer:3030/ds")
GATEWAY_HOST = os.getenv("GATEWAY_HOST", "0.0.0.0")
GATEWAY_PORT = int(os.getenv("GATEWAY_PORT", "8081"))

app = Flask(__name__)


def _nquads_to_update(body: bytes) -> tuple[str, int]:
    """Parse N-Quads and build a SPARQL Update that inserts them per named graph.

    Uses a real N-Quads parser (rather than string splitting) because RDF
    literals can legally contain whitespace, which a naive line-splitter
    would misparse.
    """
    graph = ConjunctiveGraph()
    graph.parse(data=body, format="nquads")

    statements = []
    triple_count = 0
    for context in graph.contexts():
        nt = context.serialize(format="nt").strip()
        if not nt:
            continue
        triple_count += len(context)
        statements.append(f"INSERT DATA {{ GRAPH <{context.identifier}> {{\n{nt}\n}} }}")

    if not statements:
        raise ValueError("no quads found in request body")
    return ";\n".join(statements), triple_count


@app.post("/write")
def write() -> Response:
    body = request.get_data()
    if not body:
        return Response("empty request body", status=400)

    try:
        update, triple_count = _nquads_to_update(body)
    except Exception as exc:
        return Response(f"invalid n-quads: {exc}", status=400)

    # The writer's TDB2 commit is the durability gate. Because the writer's
    # dataset is an RDF Delta DeltaDataset, Fuseki itself does not consider
    # the transaction committed until the patch has also been durably
    # recorded in the RDF Delta patch log, which is what replicates it to
    # every reader. So a 200 here means both the primary and the
    # replication log already have the write, not just the primary.
    try:
        response = requests.post(
            f"{WRITER_BASE_URL}/update",
            data=update.encode("utf-8"),
            headers={"Content-Type": "application/sparql-update"},
            timeout=30,
        )
        response.raise_for_status()
    except requests.RequestException as exc:
        return Response(f"writer commit failed: {exc}", status=502)

    return Response(f"committed triples={triple_count}", status=200)


@app.get("/healthz")
def healthz() -> Response:
    return Response("ok", status=200)


def main() -> None:
    from waitress import serve

    serve(app, host=GATEWAY_HOST, port=GATEWAY_PORT, threads=8)


if __name__ == "__main__":
    main()
