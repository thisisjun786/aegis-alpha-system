"""Run the optional local resource console."""

from __future__ import annotations

import argparse
from contextlib import suppress
from pathlib import Path

from aegis_alpha.console.registry import Registry
from aegis_alpha.console.server import ConsoleServer, tailscale_origin
from aegis_alpha.storage.paths import resolve_home

_MAX_PORT = 65535


def main() -> None:
    parser = argparse.ArgumentParser(description="Local AAS resource console")
    parser.add_argument("--home", type=Path, help="Existing private AAS installation")
    parser.add_argument(
        "--registry-home",
        type=Path,
        default=Path("~/.local/share/aegis-alpha-console"),
        help="Private resource pointers and notes directory",
    )
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument(
        "--tailscale-origin",
        type=tailscale_origin,
        help="Exact HTTPS origin of a separately configured Tailscale Serve proxy",
    )
    args = parser.parse_args()
    if not 0 <= args.port <= _MAX_PORT:
        parser.error("port must be between 0 and _MAX_PORT")
    registry = Registry(resolve_home(args.registry_home))
    with ConsoleServer(
        resolve_home(args.home), registry, args.port, remote_origin=args.tailscale_origin
    ) as server:
        print(f"AAS console: http://127.0.0.1:{server.server_port}", flush=True)  # noqa: T201
        with suppress(KeyboardInterrupt):
            server.serve_forever()


if __name__ == "__main__":
    main()
