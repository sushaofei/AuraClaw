# AGENTS.md

## Project

AuraClaw is a pure Python Managed Agent backend. The architecture source of truth is under
`docs/Managed Agent 系统架构/`.

## Commands

```bash
uv sync --extra dev
uv run uvicorn auraclaw.main:app --reload
uv run pytest
uv run ruff check .
uv run mypy src/auraclaw
```

## Architecture rules

- `domain` and `contracts` contain no FastAPI or infrastructure imports.
- Canonical Session Events are the only task fact source.
- Projections are disposable and rebuildable.
- Runtime state never changes business Session state directly.
- Orchestrator schedules resources; Coordinator owns semantic decomposition.
- Runtime Event streams are not a result-delivery guarantee.
- Tools and model providers are accessed through gateways; Agent Runtime never reads secrets.
- All writes carry tenant, command id, expected version, actor, correlation and causation context.
- Relative imports are not used across top-level modules; import through `auraclaw.*`.
- Entrypoints call `composition`; `api`, gateways, and business packages never import `composition`.
- `api` and gateways do not select concrete infrastructure adapters. Infrastructure may implement
  and import stable ports, but does not depend on `api`, gateways, or `composition`.
- Package-to-deployment alignment is maintained by the component/package/entrypoint map in
  `docs/Managed Agent 模块重构方案.md`, not by forcing a 1:1 directory topology.

## Stage completion gate

Every development stage must have a checklist in `docs/开发阶段校验清单.md`. A stage is complete
only after all applicable functional, architecture, test, security, documentation and migration
items are checked. Then commit the complete stage as one intentional Git commit and push the
current branch to `origin`. Never stage `.env`, `.history`, virtual environments, caches or secrets.
