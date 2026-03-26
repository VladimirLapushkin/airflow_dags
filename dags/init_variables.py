from __future__ import annotations

import json
from pathlib import Path

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from pendulum import datetime


VARIABLES_FILE = Path("/vars/variables.json")


with DAG(
    dag_id="load_variables_from_json",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["bootstrap", "variables"],
) as dag:

    @task
    def load_variables():
        if not VARIABLES_FILE.exists():
            raise FileNotFoundError(f"File not found: {VARIABLES_FILE}")

        with VARIABLES_FILE.open("r", encoding="utf-8") as f:
            data = json.load(f)

        if not isinstance(data, dict):
            raise ValueError("variables.json must contain a JSON object at top level")

        for key, value in data.items():
            if isinstance(value, (dict, list)):
                Variable.set(key=key, value=value, serialize_json=True)
            else:
                Variable.set(key=key, value=value)

    load_variables()
