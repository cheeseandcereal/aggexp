"""Python reference backend for the KRM component server (0017 proto).

This is the polyglot-proof for the 0019 experiment: a gRPC server in
Python that implements the same Backend service as 0017's Go
backend-note. The component server binary from 0017 cannot distinguish
between the two at runtime; the contract is the wire proto.

Single-file on purpose. In-memory storage. No dependencies beyond
grpcio + protobuf (grpcio-tools is build-time only). Lines lost to
imports and boilerplate compared to the Go version are what we're
trying to measure.
"""

from __future__ import annotations

import argparse
import json
import logging
import queue
import signal
import sys
import threading
import time
import uuid
from concurrent import futures
from datetime import datetime, timezone
from typing import Dict, Iterator, List, Optional, Tuple

import grpc

from aggexp.krm.v1 import backend_pb2 as pb
from aggexp.krm.v1 import backend_pb2_grpc as pb_grpc


# OpenAPI v3 schema identical to the Go backend's. The component server
# threads this into its defs map at startup; 0017 relies on the
# x-kubernetes-group-version-kind extension on the top-level schema.
NOTE_OPENAPI_V3: Dict = {
    "type": "object",
    "description": "Note is a free-form piece of text served by the 0019 KRM polyglot (python) backend.",
    "properties": {
        "apiVersion": {
            "type": "string",
            "description": "APIVersion defines the versioned schema of this representation of an object.",
        },
        "kind": {
            "type": "string",
            "description": "Kind is a string value representing the REST resource this object represents.",
        },
        "metadata": {
            "description": "Standard object metadata.",
            "$ref": "#/components/schemas/io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta",
        },
        "spec": {
            "type": "object",
            "description": "NoteSpec carries the caller-supplied fields. Writable; participates in server-side apply.",
            "properties": {
                "title": {
                    "type": "string",
                    "description": "Short display title. Rendered in the Title column of `kubectl get notes`.",
                },
                "body": {
                    "type": "string",
                    "description": "Free-form body text. Not rendered by kubectl get.",
                },
            },
        },
        "status": {
            "type": "object",
            "description": "NoteStatus is server-assigned. Read-only to clients other than the backend itself.",
            "properties": {
                "updatedAt": {
                    "type": "string",
                    "description": "Server-assigned last-update time (RFC 3339).",
                },
            },
        },
    },
    "x-kubernetes-group-version-kind": [
        {"group": "aggexp.io", "version": "v1", "kind": "Note"}
    ],
}


def _now_rfc3339() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _user_label(u: Optional[pb.UserInfo]) -> str:
    if u is None:
        return "<nil>"
    if not u.name:
        return "<anon>"
    return f"{u.name}[{','.join(u.groups)}]"


class NoteBackend(pb_grpc.BackendServicer):
    """In-memory Notes backend. Notes are keyed on (namespace, name).

    Watches are modeled as per-subscriber queue.Queue; the server thread
    pushes to every active queue on mutation. Initial-state replay runs
    under the same lock that buffers subsequent events, so no event is
    ever lost between the snapshot and the live stream.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._items: Dict[Tuple[str, str], Dict] = {}
        self._watchers: Dict[int, "queue.Queue[pb.WatchEvent]"] = {}
        self._next_wid = 0

    # ---- schema ----

    def GetSchema(self, request: pb.GetSchemaRequest, context: grpc.ServicerContext) -> pb.GetSchemaResponse:
        return pb.GetSchemaResponse(
            group="aggexp.io",
            version="v1",
            resource="notes",
            kind="Note",
            singular="note",
            namespaced=True,
            writable=True,
            supports_server_side_apply=True,
            openapi_v3=json.dumps(NOTE_OPENAPI_V3).encode("utf-8"),
            columns=[
                pb.TableColumn(name="Name", type="string", description="Name of the note."),
                pb.TableColumn(name="Title", type="string", description="Note title."),
                pb.TableColumn(name="Age", type="string", description="Time since creation."),
            ],
            row_fields=[".metadata.name", ".spec.title", ".metadata.creationTimestamp"],
            short_names=["nt"],
        )

    # ---- read ----

    def Get(self, request: pb.GetRequest, context: grpc.ServicerContext) -> pb.GetResponse:
        with self._lock:
            item = self._items.get((request.namespace, request.name))
        if item is None:
            context.abort(grpc.StatusCode.NOT_FOUND, f"notes.aggexp.io {request.name!r} not found")
        return pb.GetResponse(object_json=json.dumps(item).encode("utf-8"))

    def List(self, request: pb.ListRequest, context: grpc.ServicerContext) -> pb.ListResponse:
        with self._lock:
            keys = [k for k in self._items if not request.namespace or k[0] == request.namespace]
            keys.sort()
            items = [json.dumps(self._items[k]).encode("utf-8") for k in keys]
        return pb.ListResponse(items_json=items)

    # ---- write ----

    def _stamp_new(self, note: Dict, namespace: str) -> None:
        meta = note.setdefault("metadata", {})
        if namespace:
            meta["namespace"] = namespace
        note["apiVersion"] = "aggexp.io/v1"
        note["kind"] = "Note"
        meta["uid"] = str(uuid.uuid4())
        meta["creationTimestamp"] = _now_rfc3339()
        status = note.setdefault("status", {})
        status["updatedAt"] = meta["creationTimestamp"]

    def Create(self, request: pb.CreateRequest, context: grpc.ServicerContext) -> pb.CreateResponse:
        note = json.loads(request.object_json)
        meta = note.get("metadata") or {}
        name = meta.get("name")
        if not name:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "metadata.name is required")
        with self._lock:
            key = (request.namespace, name)
            if key in self._items:
                context.abort(grpc.StatusCode.ALREADY_EXISTS, f"notes.aggexp.io {name!r} already exists")
            self._stamp_new(note, request.namespace)
            self._items[key] = note
            self._broadcast_locked(pb.EVENT_ADDED, note)
        return pb.CreateResponse(object_json=json.dumps(note).encode("utf-8"))

    def Update(self, request: pb.UpdateRequest, context: grpc.ServicerContext) -> pb.UpdateResponse:
        note = json.loads(request.object_json)
        meta = note.setdefault("metadata", {})
        if request.namespace:
            meta["namespace"] = request.namespace
        if not meta.get("name"):
            meta["name"] = request.name
        note["apiVersion"] = "aggexp.io/v1"
        note["kind"] = "Note"
        with self._lock:
            key = (request.namespace, request.name)
            existing = self._items.get(key)
            if existing is None:
                if not request.force_allow_create:
                    context.abort(grpc.StatusCode.NOT_FOUND, f"notes.aggexp.io {request.name!r} not found")
                meta["uid"] = str(uuid.uuid4())
                meta["creationTimestamp"] = _now_rfc3339()
                note.setdefault("status", {})["updatedAt"] = meta["creationTimestamp"]
                self._items[key] = note
                self._broadcast_locked(pb.EVENT_ADDED, note)
                return pb.UpdateResponse(object_json=json.dumps(note).encode("utf-8"), created=True)
            meta["uid"] = existing["metadata"]["uid"]
            meta["creationTimestamp"] = existing["metadata"]["creationTimestamp"]
            note.setdefault("status", {})["updatedAt"] = _now_rfc3339()
            self._items[key] = note
            self._broadcast_locked(pb.EVENT_MODIFIED, note)
        return pb.UpdateResponse(object_json=json.dumps(note).encode("utf-8"), created=False)

    def Apply(self, request: pb.ApplyRequest, context: grpc.ServicerContext) -> pb.ApplyResponse:
        # Component server does the SSA field-manager math; we persist
        # what it hands us and emit the appropriate event type.
        note = json.loads(request.object_json)
        meta = note.setdefault("metadata", {})
        if request.namespace:
            meta["namespace"] = request.namespace
        if not meta.get("name"):
            meta["name"] = request.name
        note["apiVersion"] = "aggexp.io/v1"
        note["kind"] = "Note"
        with self._lock:
            key = (request.namespace, request.name)
            existing = self._items.get(key)
            created = existing is None
            if created:
                meta["uid"] = str(uuid.uuid4())
                meta["creationTimestamp"] = _now_rfc3339()
            else:
                meta["uid"] = existing["metadata"]["uid"]
                meta["creationTimestamp"] = existing["metadata"]["creationTimestamp"]
            note.setdefault("status", {})["updatedAt"] = _now_rfc3339()
            self._items[key] = note
            ev = pb.EVENT_ADDED if created else pb.EVENT_MODIFIED
            self._broadcast_locked(ev, note)
        logging.info("apply fm=%s user=%s name=%s created=%s",
                     request.field_manager, _user_label(request.user), request.name, created)
        return pb.ApplyResponse(object_json=json.dumps(note).encode("utf-8"), created=created)

    def Delete(self, request: pb.DeleteRequest, context: grpc.ServicerContext) -> pb.DeleteResponse:
        with self._lock:
            key = (request.namespace, request.name)
            existing = self._items.pop(key, None)
            if existing is None:
                context.abort(grpc.StatusCode.NOT_FOUND, f"notes.aggexp.io {request.name!r} not found")
            self._broadcast_locked(pb.EVENT_DELETED, existing)
        return pb.DeleteResponse(object_json=json.dumps(existing).encode("utf-8"), deleted=True)

    # ---- watch ----

    def _broadcast_locked(self, ev_type: int, obj: Dict) -> None:
        raw = json.dumps(obj).encode("utf-8")
        ev = pb.WatchEvent(type=ev_type, object_json=raw)
        for q in list(self._watchers.values()):
            try:
                q.put_nowait(ev)
            except queue.Full:
                # Drop on full — skeleton-grade; Go backend does the same.
                pass

    def Watch(self, request: pb.WatchRequest, context: grpc.ServicerContext) -> Iterator[pb.WatchEvent]:
        q: "queue.Queue[pb.WatchEvent]" = queue.Queue(maxsize=64)
        with self._lock:
            wid = self._next_wid
            self._next_wid += 1
            self._watchers[wid] = q
            initial: List[pb.WatchEvent] = []
            for (ns, _), note in self._items.items():
                if request.namespace and ns != request.namespace:
                    continue
                initial.append(pb.WatchEvent(
                    type=pb.EVENT_ADDED,
                    object_json=json.dumps(note).encode("utf-8"),
                ))
        logging.info("watch open wid=%d user=%s ns=%r", wid, _user_label(request.user), request.namespace)
        try:
            for ev in initial:
                yield ev
            while context.is_active():
                try:
                    ev = q.get(timeout=1.0)
                except queue.Empty:
                    continue
                yield ev
        finally:
            with self._lock:
                self._watchers.pop(wid, None)
            logging.info("watch close wid=%d", wid)


def serve(addr: str) -> None:
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=16),
        options=[("grpc.so_reuseport", 0)],
    )
    pb_grpc.add_BackendServicer_to_server(NoteBackend(), server)
    server.add_insecure_port(addr)
    logging.info("note-backend-py listening on %s", addr)
    server.start()

    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    while not stop.is_set():
        time.sleep(1.0)
    server.stop(grace=5).wait()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--addr", default="0.0.0.0:9090", help="gRPC listen address")
    args = ap.parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        stream=sys.stderr,
    )
    serve(args.addr)


if __name__ == "__main__":
    main()
