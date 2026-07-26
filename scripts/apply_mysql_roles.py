"""Apply deploy/mysql/roles.sql against a target database."""

from __future__ import annotations

import argparse
import asyncio

from auraclaw.infrastructure.persistence.mysql_roles import apply_mysql_roles


def main() -> int:
    parser = argparse.ArgumentParser(description="Apply AuraClaw MySQL role grants")
    parser.add_argument("--host", required=True)
    parser.add_argument("--port", type=int, default=3306)
    parser.add_argument("--admin-user", required=True)
    parser.add_argument("--admin-password", required=True)
    parser.add_argument("--database", default="auraclaw")
    parser.add_argument("--role-password", required=True)
    args = parser.parse_args()
    roles = asyncio.run(
        apply_mysql_roles(
            host=args.host,
            port=args.port,
            admin_user=args.admin_user,
            admin_password=args.admin_password,
            database=args.database,
            role_password=args.role_password,
        )
    )
    print(f"applied MySQL roles on `{args.database}`: {', '.join(roles)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
