#!/bin/sh
set -eu

repository_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$repository_root"
export AIRFLOW_HOME="$repository_root/build/airflow"
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="sqlite:///$AIRFLOW_HOME/airflow.db"
export AIRFLOW__CORE__DAGS_FOLDER="$repository_root/airflow/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__EXECUTOR=LocalExecutor
export AIRFLOW__CORE__PARALLELISM=1
export AIRFLOW__API__HOST=127.0.0.1
export AIRFLOW__CORE__DEFAULT_TIMEZONE=UTC
export AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=True
exec uv run --project "$repository_root/airflow" --locked airflow "$@"
