"""Provision the local Metabase dashboard through its pinned version's REST API."""

from __future__ import annotations

import argparse
import fcntl
import json
import secrets
import subprocess
import sys
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from uuid import NAMESPACE_URL, uuid5

import psycopg
from psycopg import sql

from olist_platform.raw.profiling import atomic_write_text

OWNER = "Managed by ecommerce-data-platform"
BI_LOGIN = "olist_metabase_reader"


class API:
    def __init__(self, url: str):
        self.url = url
        self.session = None

    def call(self, method: str, path: str, body: dict | None = None):
        headers = {"Content-Type": "application/json"}
        if self.session:
            headers["X-Metabase-Session"] = self.session
        request = Request(
            self.url + path,
            method=method,
            headers=headers,
            data=json.dumps(body).encode() if body is not None else None,
        )
        try:
            with urlopen(request, timeout=120) as response:
                payload = response.read()
                return json.loads(payload) if payload else None
        except HTTPError as error:
            raise RuntimeError(
                f"Metabase {method} {path}: HTTP {error.code}; inspect container logs"
            ) from None
        except URLError:
            raise RuntimeError(
                "Local Metabase is unavailable; run make metabase-up"
            ) from None


def credentials(path: Path, initialized: bool) -> dict:
    if path.exists():
        if path.stat().st_mode & 0o077:
            raise RuntimeError("Metabase credential file must have permissions 0600")
        return json.loads(path.read_text())
    if initialized:
        raise RuntimeError(
            "Metabase is initialized but local credentials are missing; restore build/metabase/credentials.json"
        )
    value = {
        "email": "admin@olist.example",
        "password": secrets.token_urlsafe(32) + "aA1!",
        "reader_password": secrets.token_urlsafe(32),
    }
    atomic_write_text(path, json.dumps(value, indent=2) + "\n")
    path.chmod(0o600)
    return value


def configure_reader(connection: psycopg.Connection, password: str) -> None:
    with connection.transaction():
        existing = connection.execute(
            "SELECT 1 FROM pg_roles WHERE rolname=%s", (BI_LOGIN,)
        ).fetchone()
        if not existing:
            connection.execute(
                sql.SQL("CREATE ROLE {} LOGIN PASSWORD {}").format(
                    sql.Identifier(BI_LOGIN), sql.Literal(password)
                )
            )
        else:
            # Do not repurpose a pre-existing privileged or unrelated login.
            flags = connection.execute(
                "SELECT rolsuper OR rolcreatedb OR rolcreaterole OR rolreplication OR rolbypassrls FROM pg_roles WHERE rolname=%s",
                (BI_LOGIN,),
            ).fetchone()[0]
            memberships = connection.execute(
                "SELECT parent.rolname FROM pg_auth_members m JOIN pg_roles parent ON parent.oid=m.roleid JOIN pg_roles member ON member.oid=m.member WHERE member.rolname=%s",
                (BI_LOGIN,),
            ).fetchall()
            if flags or memberships != [("olist_bi_reader",)]:
                raise RuntimeError(
                    "Existing BI login is not the managed restricted reader"
                )
        connection.execute(
            sql.SQL("GRANT olist_bi_reader TO {}").format(sql.Identifier(BI_LOGIN))
        )
        connection.execute(
            sql.SQL("ALTER ROLE {} SET default_transaction_read_only = on").format(
                sql.Identifier(BI_LOGIN)
            )
        )
        connection.execute(
            sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(
                sql.Identifier(connection.info.dbname), sql.Identifier(BI_LOGIN)
            )
        )


def native_card(spec: dict, database_id: int, collection_id: int) -> dict:
    tags = {
        name: {
            "id": str(uuid5(NAMESPACE_URL, "olist." + name)),
            "name": name,
            "display-name": name.replace("_", " ").title(),
            "type": "date",
            "required": False,
        }
        for name in ("start_date", "end_date")
        if "{{" + name + "}}" in spec["query"]
    }
    return {
        "name": spec["name"],
        "description": OWNER + ": " + spec["key"],
        "collection_id": collection_id,
        "display": spec["display"],
        "visualization_settings": spec["visualization_settings"],
        "dataset_query": {
            "database": database_id,
            "type": "native",
            "native": {"query": spec["query"], "template-tags": tags},
        },
    }


def find(items: list[dict], name: str) -> dict | None:
    matches = [item for item in items if item["name"] == name]
    if len(matches) > 1:
        raise RuntimeError(f"Duplicate managed Metabase name: {name}")
    return matches[0] if matches else None


def provision(api: API, services: dict, secret: dict, definition: dict) -> dict:
    properties = api.call("GET", "/api/session/properties")
    if properties.get("has-user-setup"):
        session = api.call(
            "POST",
            "/api/session",
            {"username": secret["email"], "password": secret["password"]},
        )
    else:
        session = api.call(
            "POST",
            "/api/setup",
            {
                "token": properties["setup-token"],
                "user": {
                    "email": secret["email"],
                    "password": secret["password"],
                    "first_name": "Olist",
                    "last_name": "Local",
                },
                "prefs": {"site_name": "Olist Analytics", "allow_tracking": False},
            },
        )
    api.session = session["id"]
    collection = find(api.call("GET", "/api/collection"), "Olist Core Analytics")
    if collection and collection.get("description") != OWNER:
        raise RuntimeError("Metabase collection name belongs to unmanaged content")
    collection = collection or api.call(
        "POST",
        "/api/collection",
        {"name": "Olist Core Analytics", "description": OWNER, "color": "#509EE3"},
    )
    details = {
        "host": "postgres",
        "port": 5432,
        "dbname": services["postgres"]["environment"]["POSTGRES_DB"],
        "user": BI_LOGIN,
        "password": secret["reader_password"],
        "ssl": False,
        "schema-filters-type": "inclusion",
        "schema-filters-patterns": "analytics",
    }
    databases = api.call("GET", "/api/database")["data"]
    database = find(databases, "Olist Analytics Reader")
    if database:
        current = api.call("GET", f"/api/database/{database['id']}")
        if any(
            current["details"].get(k) != details[k] for k in ("host", "dbname", "user")
        ):
            raise RuntimeError("Metabase database name belongs to another connection")
    database = database or api.call(
        "POST",
        "/api/database",
        {
            "name": "Olist Analytics Reader",
            "engine": "postgres",
            "details": details,
            "is_full_sync": False,
            "auto_run_queries": False,
        },
    )
    items = api.call("GET", f"/api/collection/{collection['id']}/items")["data"]
    dashboard = find(
        [x for x in items if x["model"] == "dashboard"], definition["name"]
    )
    dashboard = dashboard or api.call(
        "POST",
        "/api/dashboard",
        {
            "name": definition["name"],
            "collection_id": collection["id"],
            "description": definition["description"],
        },
    )
    current = api.call("GET", f"/api/dashboard/{dashboard['id']}")
    old_cards = {
        d["card_id"]: d["id"] for d in current["dashcards"] if d.get("card_id")
    }
    text = next((d for d in current["dashcards"] if not d.get("card_id")), {})
    layout = [
        {
            "id": text.get("id", -1),
            "card_id": None,
            "row": 0,
            "col": 0,
            "size_x": 24,
            "size_y": 6,
            "visualization_settings": {
                "virtual_card": {"display": "text"},
                "text": definition["policy"],
            },
        }
    ]
    results = {}
    for index, spec in enumerate(definition["cards"], 2):
        existing = find([x for x in items if x["model"] == "card"], spec["name"])
        body = native_card(spec, database["id"], collection["id"])
        if existing:
            card = api.call("GET", f"/api/card/{existing['id']}")
            if card.get("description") != body["description"]:
                raise RuntimeError("Metabase card name belongs to unmanaged content")
            card = api.call("PUT", f"/api/card/{existing['id']}", body)
        else:
            card = api.call("POST", "/api/card", body)
        row, col, width, height = spec["layout"]
        mappings = [
            {
                "parameter_id": key,
                "card_id": card["id"],
                "target": ["variable", ["template-tag", key + "_date"]],
            }
            for key in ("start", "end")
            if key + "_date" in body["dataset_query"]["native"]["template-tags"]
        ]
        layout.append(
            {
                "id": old_cards.get(card["id"], -index),
                "card_id": card["id"],
                "row": row,
                "col": col,
                "size_x": width,
                "size_y": height,
                "parameter_mappings": mappings,
            }
        )
        result = api.call(
            "POST",
            f"/api/card/{card['id']}/query",
            {"parameters": [], "ignore_cache": True},
        )
        if result.get("status") != "completed":
            raise RuntimeError(f"Metabase query failed: {spec['key']}")
        results[spec["key"]] = {"card_id": card["id"], "rows": result["data"]["rows"]}
    parameters = [
        {
            "id": key,
            "slug": key,
            "name": key.title() + " date (inclusive)",
            "type": "date/single",
        }
        for key in ("start", "end")
    ]
    api.call(
        "PUT",
        f"/api/dashboard/{dashboard['id']}",
        {
            "dashcards": layout,
            "parameters": parameters,
            "description": definition["description"],
        },
    )
    return {
        "dashboard_id": dashboard["id"],
        "dashboard_url": api.url + f"/dashboard/{dashboard['id']}",
        "results": results,
    }


def main() -> int:
    argparse.ArgumentParser(description=__doc__).parse_args()
    root = Path.cwd()
    directory = root / "build/metabase"
    directory.mkdir(parents=True, exist_ok=True)
    with (directory / "bootstrap.lock").open("a") as guard:
        try:
            fcntl.flock(guard, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError("Another Metabase setup is active") from None
        services = json.loads(
            subprocess.check_output(
                [
                    "docker",
                    "compose",
                    "-f",
                    "infrastructure/docker-compose.yml",
                    "--profile",
                    "*",
                    "config",
                    "--format",
                    "json",
                ]
            )
        )["services"]
        api = API(
            "http://127.0.0.1:" + str(services["metabase"]["ports"][0]["published"])
        )
        properties = api.call("GET", "/api/session/properties")
        secret = credentials(
            directory / "credentials.json", properties.get("has-user-setup")
        )
        postgres = services["postgres"]
        env = postgres["environment"]
        with psycopg.connect(
            host="127.0.0.1",
            port=postgres["ports"][0]["published"],
            dbname=env["POSTGRES_DB"],
            user=env["POSTGRES_USER"],
            password=env["POSTGRES_PASSWORD"],
            autocommit=True,
        ) as c:
            available = c.execute(
                "SELECT available FROM analytics.bi_publication_status"
            ).fetchone()
            if not available or not available[0]:
                raise RuntimeError(
                    "Complete/resume the latest pipeline before provisioning business analytics"
                )
            configure_reader(c, secret["reader_password"])
        definition = json.loads((root / "config/metabase_dashboard.json").read_text())
        result = provision(api, services, secret, definition)
        atomic_write_text(
            directory / "dashboard.json", json.dumps(result, indent=2) + "\n"
        )
        print(f"Dashboard: {result['dashboard_url']}")
        print("Local login credentials: build/metabase/credentials.json (0600)")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (RuntimeError, OSError, psycopg.Error, ValueError, KeyError) as error:
        message = (
            str(error) if isinstance(error, RuntimeError) else type(error).__name__
        )
        print(f"Metabase setup failed: {message}", file=sys.stderr)
        raise SystemExit(1) from None
