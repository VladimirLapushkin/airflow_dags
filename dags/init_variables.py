from __future__ import annotations

import json

import boto3
import botocore
import yandexcloud
from airflow.configuration import conf
from airflow.decorators import dag, task
from airflow.models import Variable
from pendulum import datetime


@dag(
    dag_id="init_variables_from_object_storage",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["bootstrap", "variables"],
)
def init_variables_from_object_storage():

    @task
    def load_variables():
        bucket = conf.get("bootstrap", "bucket")
        key = conf.get("bootstrap", "key", fallback="vars/variables.json")
        endpoint = conf.get("bootstrap", "endpoint", fallback="https://storage.yandexcloud.net")

        sdk = yandexcloud.SDK()

        def provide_cloud_auth_header(request, **kwargs):
            request.headers.add_header(
                "X-YaCloud-SubjectToken",
                sdk._channels._token_requester.get_token(),
            )

        session = boto3.session.Session()
        session.events.register("request-created.s3.*", provide_cloud_auth_header)

        s3 = session.client(
            service_name="s3",
            endpoint_url=endpoint,
            config=botocore.config.Config(
                signature_version=botocore.UNSIGNED,
                retries={"max_attempts": 5, "mode": "standard"},
            ),
        )

        obj = s3.get_object(Bucket=bucket, Key=key)
        data = json.loads(obj["Body"].read().decode("utf-8"))

        if not isinstance(data, dict):
            raise ValueError("variables.json must contain a JSON object at top level")

        loaded = 0
        for var_key, var_value in data.items():
            if isinstance(var_value, (dict, list)):
                Variable.set(var_key, var_value, serialize_json=True)
            else:
                Variable.set(var_key, var_value)
            loaded += 1

        return {"loaded": loaded, "bucket": bucket, "key": key}

    load_variables()


dag = init_variables_from_object_storage()
