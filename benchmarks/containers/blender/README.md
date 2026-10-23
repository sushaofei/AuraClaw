# Isolated Blender admission

This image is an opt-in MCP-Universe environment, not a SkillsBench dependency.
It contains Blender 4.4.3, the frozen MCP-Universe `blender_addon.py` and Blender
`server.py`. Do not copy the task evaluators, gold files, credentials, repository
checkout or host home directory into the agent image.

Prepare a private build context containing:

- `Containerfile`, `start.py` from this directory;
- `blender-4.4.3-linux-x64.tar.xz`, verified against the official release SHA-256;
- `addon.py` and `server.py` from the revision in `components/catalog.py`;
- an offline `wheels/` directory for the base image's Python ABI and Blender's
  bundled Python 3.11. Pin MCP 1.13.1 and requests 2.32.4; retain the resolved
  dependency inventory and image digest with the private admission receipt.

Build with `--pull=never --build-arg BASE_IMAGE=<verified immutable image ID>`.
`probe.py` requires the resulting immutable image ID and uses a fresh container
with no network, no host mounts, a read-only root, no capabilities, non-root UID,
resource limits and disposable temporary filesystems. It checks MCP discovery,
creates and inspects a probe cube, and saves a scene. This is environmental
admission only; it does not produce an official benchmark score.

The official per-task evaluator must run outside the agent container on copied
scene artifacts. Its scripts and results must never be visible to the agent.
Runtime/Gateway binding, official evaluator admission and batch supervision are
separate B2 requirements. Do not start the formal cohort on the basis of this
probe alone.
