"""Local convenience runner; underlying commands retain validation and retry rules."""

from __future__ import annotations

import fcntl
import json
import os
import re
import shutil
import subprocess
from pathlib import Path
from uuid import uuid4

RUN_CONTEXT = None
CURRENT_TASK = None

CORE_TABLES = ("customers", "orders", "order_items", "order_payments")


def audit(command: str, *options: str, run_id: str | None = None) -> dict:
    identity = run_id or RUN_CONTEXT["run_id"]
    output = run(
        "uv",
        "run",
        "--locked",
        "python",
        "-m",
        "olist_platform.pipeline.cli",
        "--run-id",
        identity,
        command,
        *options,
        capture=True,
    )
    return json.loads(output)


def run(*arguments: str, capture: bool = False) -> str:
    global CURRENT_TASK
    name = None
    if RUN_CONTEXT is not None and "olist_platform.pipeline.cli" not in arguments:
        if "--table" in arguments:
            table = arguments[arguments.index("--table") + 1]
            module = arguments[arguments.index("-m") + 1].split(".")[1]
            verb = next(
                (
                    word
                    for word in ("extract", "process", "register", "resolve")
                    if word in arguments
                ),
                "load",
            )
            name = f"{table}.{module}.{verb}"
        if name:
            CURRENT_TASK = name
            audit(
                "task",
                "--attempt",
                str(RUN_CONTEXT["attempt"]),
                "--name",
                name,
                "--status",
                "RUNNING",
            )
    result = subprocess.run(
        arguments, check=True, text=True, stdout=subprocess.PIPE if capture else None
    )
    if name:
        audit(
            "task",
            "--attempt",
            str(RUN_CONTEXT["attempt"]),
            "--name",
            name,
            "--status",
            "SUCCESS",
        )
        CURRENT_TASK = None
    return result.stdout if capture else ""


def main() -> None:
    global RUN_CONTEXT, CURRENT_TASK
    RUN_CONTEXT = CURRENT_TASK = None
    reports = Path("build/reports/dataset")
    reports.mkdir(parents=True, exist_ok=True)
    with (reports / "runner.lock").open("a") as guard:
        try:
            fcntl.flock(guard, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError("Another local dataset runner is active") from None
        try:
            execute()
        except BaseException:
            if RUN_CONTEXT is not None:
                try:
                    audit(
                        "fail",
                        "--attempt",
                        str(RUN_CONTEXT["attempt"]),
                        "--name",
                        CURRENT_TASK or "verification",
                    )
                except (subprocess.SubprocessError, OSError, ValueError):
                    print(
                        "Pipeline failure audit unavailable; retain the pending run ID.",
                        flush=True,
                    )
            raise
        finally:
            RUN_CONTEXT = CURRENT_TASK = None


def execute() -> None:
    global RUN_CONTEXT, CURRENT_TASK
    manifest = json.loads(Path("config/raw_dataset_manifest.json").read_text())
    missing = [
        f["name"]
        for f in manifest["files"]
        if not (Path("data/raw") / f["name"]).is_file()
    ]
    for table in CORE_TABLES:
        name = f"olist_{table}_dataset.csv"
        print(
            f"[{'SKIP' if name in missing else 'FOUND'}] {table}: data/raw/{name}",
            flush=True,
        )
    if missing:
        print(
            "[SKIP] Source seed and downstream: the loader requires the complete 9-file snapshot."
        )
        for name in missing:
            print(f"  Missing: data/raw/{name}")
        return

    if not Path(".env").exists():
        shutil.copyfile(".env.example", ".env")
    run("uv", "sync", "--locked", "--inexact")
    compose = ("docker", "compose", "-f", "infrastructure/docker-compose.yml")
    # Let Compose parse .env; never execute its contents as shell code.
    config = json.loads(
        run(*compose, "--profile", "*", "config", "--format", "json", capture=True)
    )["services"]
    postgres = config["postgres"]["environment"]
    loader = config["source-loader"]["environment"]
    minio = config["minio"]["environment"]
    ports = {
        name: str(config[name]["ports"][0]["published"])
        for name in ("postgres", "minio")
    }
    os.environ.update(
        PGHOST="127.0.0.1",
        PGPORT=ports["postgres"],
        PGDATABASE=postgres["POSTGRES_DB"],
        PGUSER=loader["SOURCE_LOADER_USER"],
        PGPASSWORD=loader["SOURCE_LOADER_PASSWORD"],
        AWS_ACCESS_KEY_ID=minio["MINIO_ROOT_USER"],
        AWS_SECRET_ACCESS_KEY=minio["MINIO_ROOT_PASSWORD"],
        BRONZE_S3_ENDPOINT=f"http://127.0.0.1:{ports['minio']}",
        BRONZE_S3_BUCKET="olist-bronze",
        DBT_ENV_SECRET_PASSWORD=loader["SOURCE_LOADER_PASSWORD"],
        DBT_READER_USER=config["source-check"]["environment"]["PGUSER"],
        DBT_SEND_ANONYMOUS_USAGE_STATS="false",
    )
    # Match the services we just configured, even if the caller exported another DSN.
    os.environ.pop("BRONZE_SOURCE_DSN", None)
    os.environ.pop("WAREHOUSE_DSN", None)
    run("make", "seed-source", "verify-source")
    run("make", "bronze-up", "init-bronze")

    java_home = os.environ.get("JAVA_HOME")
    if not java_home:
        candidates = sorted(Path("/usr/lib/jvm").glob("java-17*/bin/java"))
        if candidates:
            os.environ["JAVA_HOME"] = str(candidates[0].parent.parent)
    java = (
        str(Path(os.environ["JAVA_HOME"]) / "bin/java")
        if os.environ.get("JAVA_HOME")
        else "java"
    )
    silver = shutil.which(java) is not None
    if silver:
        run("uv", "sync", "--locked", "--extra", "processing", "--extra", "analytics")
    else:
        print(
            "[SKIP] Silver: Java is unavailable; install Java 17 to enable it.",
            flush=True,
        )
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

    reports = Path("build/reports/dataset")
    reports.mkdir(parents=True, exist_ok=True)
    pending = reports / "pending-run.txt"
    run_id = (
        pending.read_text().strip() if pending.exists() else f"dataset_{uuid4().hex}"
    )
    if not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_-]{0,95}", run_id):
        raise ValueError("Invalid saved dataset run ID")
    pending.write_text(run_id + "\n")
    print(f"[RUN] {run_id}; rerun make dataset after failure to resume", flush=True)
    if silver:
        started = audit("begin", run_id=run_id)
        if started["status"] == "NOOP":
            pending.unlink()
            print(f"[DONE] Pipeline already committed: {run_id}", flush=True)
            return
        RUN_CONTEXT = {"run_id": run_id, "attempt": started["attempt"]}
    snapshots = {}
    for table in CORE_TABLES:
        bronze = json.loads(
            run(
                "uv",
                "run",
                "--locked",
                "python",
                "-m",
                "olist_platform.bronze.cli",
                "--table",
                table,
                "extract",
                "--run-id",
                run_id,
                *(["--full-snapshot"] if table != "customers" else []),
                capture=True,
            )
        )
        (reports / f"{run_id}_{table}_bronze.json").write_text(
            json.dumps(bronze, indent=2) + "\n"
        )
        print(f"[OK] {table} Bronze: {bronze['manifest']['prefix']}", flush=True)
        if not silver:
            continue
        arguments = [
            "uv",
            "run",
            "--locked",
            "--extra",
            "processing",
            "python",
            "-m",
            "olist_platform.silver.cli",
            "--table",
            table,
            "process",
            "--bronze-prefix",
            bronze["manifest"]["prefix"],
            "--run-id",
            run_id,
        ]
        if table == "orders":
            arguments.extend(["--customer-snapshot", snapshots["customers"]])
        elif table in ("order_items", "order_payments"):
            arguments.extend(["--order-snapshot", snapshots["orders"]])
        output = json.loads(run(*arguments, capture=True))
        (reports / f"{run_id}_{table}_silver.json").write_text(
            json.dumps(output, indent=2) + "\n"
        )
        snapshots[table] = output["manifest"]["prefix"]
        print(f"[OK] {table} Silver: {output['manifest']['prefix']}", flush=True)
        loaded = json.loads(
            run(
                "uv",
                "run",
                "--locked",
                "--extra",
                "processing",
                "python",
                "-m",
                "olist_platform.warehouse.cli",
                "--table",
                table,
                "--silver-prefix",
                output["manifest"]["prefix"],
                "--run-id",
                run_id,
                capture=True,
            )
        )
        (reports / f"{run_id}_{table}_warehouse.json").write_text(
            json.dumps(loaded, indent=2) + "\n"
        )
        print(f"[OK] {table} staging: {loaded['counts']}", flush=True)
        if table != "customers":
            deferred_cli = [
                "uv",
                "run",
                "--locked",
                "--extra",
                "processing",
                "python",
                "-m",
                "olist_platform.deferred.cli",
                "--table",
                table,
            ]
            registered = json.loads(
                run(
                    *deferred_cli,
                    "register",
                    "--silver-prefix",
                    output["manifest"]["prefix"],
                    "--run-id",
                    run_id,
                    capture=True,
                )
            )
            parent = snapshots["customers" if table == "orders" else "orders"]
            resolved = json.loads(
                run(
                    *deferred_cli,
                    "resolve",
                    "--parent-snapshot",
                    parent,
                    "--run-id",
                    run_id,
                    capture=True,
                )
            )
            for suffix, result in (
                ("deferred_registration", registered),
                ("deferred_resolution", resolved),
            ):
                (reports / f"{run_id}_{table}_{suffix}.json").write_text(
                    json.dumps(result, indent=2) + "\n"
                )
            print(f"[OK] {table} deferred: {resolved['routing_counts']}", flush=True)
    if silver:
        CURRENT_TASK = "dbt.build"
        finished = audit(
            "finish",
            "--attempt",
            str(RUN_CONTEXT["attempt"]),
            "--dbt-target",
            str((reports / f"{run_id}_dbt").resolve()),
        )
        (reports / f"{run_id}_pipeline.json").write_text(
            json.dumps(finished, indent=2) + "\n"
        )
        CURRENT_TASK = None
        print(f"[OK] dbt/reconciliation/watermarks: {finished['status']}", flush=True)
    else:
        print(
            "[PARTIAL] Bronze only; keep this run ID and install Java 17 to resume the full pipeline.",
            flush=True,
        )
        return
    pending.unlink()
    print(f"[DONE] Reports: {reports}/{run_id}_*.json", flush=True)


if __name__ == "__main__":
    try:
        main()
    except RuntimeError as error:
        raise SystemExit(str(error)) from None
    except subprocess.CalledProcessError as error:
        raise SystemExit(
            f"Dataset command failed (exit {error.returncode}); fix the error and rerun make dataset."
        ) from None
