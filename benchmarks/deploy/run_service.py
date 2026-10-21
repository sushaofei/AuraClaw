"""Container entrypoint for the opt-in development evaluation topology only."""

from __future__ import annotations

import argparse

from auraclaw.composition.cli import _run_service_process
from auraclaw.composition.service_spec import (
    DATABASE_SERVICES,
    KAFKA_SERVICES,
    SERVICE_BY_COMMAND,
    service_spec,
)
from auraclaw.config import get_settings


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("service", choices=tuple(SERVICE_BY_COMMAND))
    args = parser.parse_args()
    settings = get_settings()
    if settings.deployment_profile != "development":
        raise SystemExit("Evaluation entrypoint requires the development profile")
    spec = service_spec(args.service, settings)
    if spec.name in DATABASE_SERVICES and not settings.sql_storage_enabled:
        raise SystemExit("Evaluation owner services require SQL storage")
    if spec.name in KAFKA_SERVICES and not settings.kafka_enabled:
        raise SystemExit("Evaluation streaming requires Kafka")
    if spec.name == "agent-runtime" and settings.model_api_key is not None:
        raise SystemExit("Evaluation Runtime must not receive Provider credentials")
    # Reuse the exact child-process entrypoint used by `auraclaw serve`, including
    # migration checks, service builders, logging, worker lifecycle and shutdown.
    _run_service_process(args.service, "0.0.0.0", spec.port, settings.log_level.lower(), None)


if __name__ == "__main__":
    main()
