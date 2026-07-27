"""Proxy 127.0.0.1:9092 to the real Kafka broker.

Some Kafka installs advertise listeners as localhost:9092. Clients inside Docker
reach the bootstrap host, then fail when following metadata to localhost.
Run this before the service process so advertised localhost stays reachable.
"""

from __future__ import annotations

import os
import select
import socket
import subprocess
import sys
import threading


def _pipe(src: socket.socket, dst: socket.socket) -> None:
    try:
        while True:
            data = src.recv(65536)
            if not data:
                break
            dst.sendall(data)
    except OSError:
        pass
    finally:
        for sock in (src, dst):
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                sock.close()
            except OSError:
                pass


def _handle(client: socket.socket, upstream_host: str, upstream_port: int) -> None:
    try:
        upstream = socket.create_connection((upstream_host, upstream_port), timeout=10)
    except OSError:
        client.close()
        return
    threading.Thread(target=_pipe, args=(client, upstream), daemon=True).start()
    _pipe(upstream, client)


def _serve(upstream_host: str, upstream_port: int) -> None:
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("127.0.0.1", 9092))
    server.listen(128)
    server.settimeout(1.0)
    while True:
        try:
            client, _ = server.accept()
        except socket.timeout:
            continue
        threading.Thread(
            target=_handle,
            args=(client, upstream_host, upstream_port),
            daemon=True,
        ).start()


def main() -> int:
    upstream_host = os.environ.get("KAFKA_HOST") or "127.0.0.1"
    upstream_port = int(os.environ.get("KAFKA_PORT") or "9092")
    if upstream_host in {"127.0.0.1", "localhost"}:
        print(
            "kafka localhost proxy refused: KAFKA_HOST must be the real broker",
            file=sys.stderr,
        )
        return 2
    threading.Thread(
        target=_serve, args=(upstream_host, upstream_port), daemon=True
    ).start()
    # Give the listener a moment before the app bootstraps.
    select.select([], [], [], 0.2)
    # Point the process at the local proxy. Brokers that advertise
    # localhost:9092 then stay reachable inside the container.
    os.environ["KAFKA_HOST"] = "127.0.0.1"
    os.environ["KAFKA_PORT"] = "9092"
    return subprocess.call(["auraclaw", *sys.argv[1:]])


if __name__ == "__main__":
    raise SystemExit(main())
