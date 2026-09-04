from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from urllib.parse import quote

from dotenv import dotenv_values

DEFAULT_DATABASE = "chaintower_agent"
CURRENT_MIGRATION_TARGET = "0064"


def _required(values: dict[str, str | None], *names: str) -> str:
    for name in names:
        value = values.get(name)
        if value:
            return str(value)
    raise ValueError(f"missing {' or '.join(names)}")


def _replace_assignment(lines: list[str], key: str, value: str) -> None:
    prefix = f"{key}="
    for index, line in enumerate(lines):
        if line.startswith(prefix):
            lines[index] = f"{prefix}{value}"
            return
    lines.append(f"{prefix}{value}")


def _database_block(values: dict[str, str | None]) -> list[str]:
    host = _required(values, "KINGBASE_HOST")
    port = _required(values, "KINGBASE_PORT")
    user = _required(values, "KINGBASE_DB_USER", "KINGBASE_USER")
    password = _required(values, "KINGBASE_DB_PWD", "KINGBASE_PWD")
    database = str(values.get("KINGBASE_AURACLAW_DB") or DEFAULT_DATABASE)
    url = (
        "postgresql+asyncpg://"
        f"{quote(user, safe='')}:{quote(password, safe='')}@{host}:{port}/"
        f"{quote(database, safe='')}"
    )
    return [
        f"# --- Database（KingBase V9 @ {host}:{port}，PostgreSQL 兼容模式）---",
        "# 由 scripts/sync_kingbase_env.py 从 gitignored .host.env 生成；勿手工复制密码。",
        "AURACLAW_DB_DIALECT=postgres",
        f"DB_HOST={host}",
        f"DB_PORT={port}",
        f"DB_USER={user}",
        f"DB_PWD={password}",
        f"DB_NAME={database}",
        f"AURACLAW_DATABASE_URL={url}",
        f"AURACLAW_MIGRATION_DATABASE_URL={url}",
        "",
    ]


def sync_environment(path: Path, host_values: dict[str, str | None]) -> None:
    original = path.read_text(encoding="utf-8")
    lines = original.splitlines()
    start = next(
        (index for index, line in enumerate(lines) if line.startswith("# --- Database")),
        None,
    )
    if start is None:
        raise ValueError(f"{path} has no Database section")
    end = next(
        (
            index
            for index in range(start + 1, len(lines))
            if lines[index].startswith("# --- Workload tokens")
        ),
        None,
    )
    if end is None:
        raise ValueError(f"{path} has no Workload tokens section after Database")
    lines[start:end] = _database_block(host_values)
    _replace_assignment(lines, "AURACLAW_STORAGE_BACKEND", "kingbase")
    _replace_assignment(lines, "AURACLAW_MIGRATIONS_DIRECTORY", "/app/migrations")
    _replace_assignment(lines, "AURACLAW_MIGRATE_TARGET", CURRENT_MIGRATION_TARGET)
    temporary = path.with_name(f".{path.name}.kingbase.tmp")
    temporary.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    os.chmod(temporary, 0o600)
    temporary.replace(path)


def ensure_database_name(path: Path, values: dict[str, str | None]) -> None:
    if values.get("KINGBASE_AURACLAW_DB"):
        return
    original = path.read_text(encoding="utf-8").rstrip()
    temporary = path.with_name(f".{path.name}.kingbase.tmp")
    temporary.write_text(
        original + f"\nKINGBASE_AURACLAW_DB={DEFAULT_DATABASE}\n",
        encoding="utf-8",
    )
    os.chmod(temporary, 0o600)
    temporary.replace(path)
    values["KINGBASE_AURACLAW_DB"] = DEFAULT_DATABASE


def main() -> int:
    parser = argparse.ArgumentParser(
        description="synchronize test/production database settings from .host.env KingBase values"
    )
    parser.add_argument("--host-env", default=".host.env")
    parser.add_argument(
        "--env-file",
        action="append",
        dest="env_files",
        help="target env file; repeat for multiple files (default: .env.test and .env.prod)",
    )
    args = parser.parse_args()
    host_path = Path(args.host_env)
    if not host_path.is_file():
        print(f"KingBase env sync failed: missing {host_path}")
        return 1
    host_values = dict(dotenv_values(host_path))
    ensure_database_name(host_path, host_values)
    targets = tuple(Path(item) for item in (args.env_files or (".env.test", ".env.prod")))
    try:
        for target in targets:
            if not target.is_file():
                raise ValueError(f"missing {target}")
            sync_environment(target, host_values)
    except ValueError as exc:
        print(f"KingBase env sync failed: {exc}")
        return 1
    print("KingBase env sync completed for " + ", ".join(str(item) for item in targets))
    return 0


if __name__ == "__main__":
    sys.exit(main())
