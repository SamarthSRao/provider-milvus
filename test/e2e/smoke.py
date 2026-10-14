"""Milvus data-plane smoke test against an Instance's connection Secret.

MODE=full (default) writes, searches and drops a collection.
MODE=write leaves the collection behind; MODE=verify checks it is still there
and searchable, then drops it. Run write and verify around an upgrade or
restart to prove data survives it. Exits non-zero on any failure.
"""

import os
import random
import sys

from pymilvus import DataType, MilvusClient
from pymilvus.client.types import LoadState

COLLECTION = "openeverest_smoke"
DIM = 8
ROWS = 200
PROBE_ID = 42


def vectors() -> list[dict]:
    rng = random.Random(7)
    return [{"id": i, "vector": [rng.random() for _ in range(DIM)]} for i in range(ROWS)]


def write(client: MilvusClient) -> None:
    if client.has_collection(COLLECTION):
        client.drop_collection(COLLECTION)
    schema = client.create_schema(auto_id=False)
    schema.add_field("id", DataType.INT64, is_primary=True)
    schema.add_field("vector", DataType.FLOAT_VECTOR, dim=DIM)
    index = client.prepare_index_params()
    index.add_index(field_name="vector", index_type="AUTOINDEX", metric_type="L2")
    client.create_collection(COLLECTION, schema=schema, index_params=index)
    client.insert(COLLECTION, vectors())
    client.flush(COLLECTION)
    # Loading with several replicas pins one query and one streaming node each.
    if replicas := int(os.environ.get("REPLICAS", "0")):
        client.load_collection(COLLECTION, replica_number=replicas)


def verify(client: MilvusClient) -> bool:
    if client.get_load_state(COLLECTION)["state"] != LoadState.Loaded:
        client.load_collection(COLLECTION)
    hits = client.search(COLLECTION, data=[vectors()[PROBE_ID]["vector"]], limit=3, output_fields=["id"])
    top = hits[0][0]["id"]
    count = client.query(COLLECTION, filter="", output_fields=["count(*)"])[0]["count(*)"]
    print(f"top hit: {top}, row count: {count}")
    return top == PROBE_ID and count == ROWS


def main() -> int:
    mode = os.environ.get("MODE", "full")
    client = MilvusClient(uri=os.environ["MILVUS_URI"], token=os.environ["MILVUS_TOKEN"])
    print("server version:", client.get_server_version())

    if mode in ("full", "write"):
        write(client)
    ok = True
    if mode in ("full", "verify"):
        ok = verify(client)
        client.drop_collection(COLLECTION)

    print("PASS" if ok else f"FAIL: expected top hit {PROBE_ID} and {ROWS} rows")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
