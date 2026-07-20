"""Stable ASGI import path backed by the API composition root."""

from auraclaw.composition.api import app, create_app, lifespan

__all__ = ["app", "create_app", "lifespan"]
