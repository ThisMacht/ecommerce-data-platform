"""Import the real DAG against the isolated, pinned Airflow runtime."""

import unittest
from datetime import timedelta
from pathlib import Path

from airflow.models.dagbag import DagBag
from airflow.serialization.serialized_objects import SerializedDAG

ROOT = Path(__file__).resolve().parents[2]


class DagTest(unittest.TestCase):
    def test_real_import_serialization_schedule_and_retry_context(self):
        bag = DagBag(dag_folder=str(ROOT / "airflow/dags"), include_examples=False)
        self.assertEqual(bag.import_errors, {})
        self.assertEqual(set(bag.dags), {"olist_core_full_snapshot"})
        dag = bag.dags["olist_core_full_snapshot"]
        self.assertEqual(dag.max_active_runs, 1)
        self.assertFalse(dag.catchup)
        self.assertTrue(dag.is_paused_upon_creation)
        self.assertEqual(dag.schedule, "@daily")
        self.assertEqual(dag.task_ids, ["run_core_pipeline"])
        task = dag.get_task("run_core_pipeline")
        self.assertEqual(task.retries, 2)
        self.assertEqual(task.retry_delay, timedelta(minutes=1))
        self.assertFalse(task.do_xcom_push)
        self.assertEqual(task.skip_on_exit_code, [])
        self.assertEqual(task.cwd, str(ROOT))
        self.assertEqual(task.bash_command, "exec python3 scripts/make_dataset.py")
        task.render_template_fields({"dag": dag, "run_id": "manual__retry '$(false)'"})
        self.assertEqual(task.env["OLIST_AIRFLOW_RUN_ID"], "manual__retry '$(false)'")
        self.assertEqual(task.env["OLIST_AIRFLOW_DAG_ID"], dag.dag_id)
        self.assertNotIn("manual__retry", task.bash_command)
        self.assertIsInstance(SerializedDAG.to_dict(dag), dict)
