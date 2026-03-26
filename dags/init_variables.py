from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
import json
from datetime import datetime


S3_BUCKET_NAME = Variable.get("S3_BUCKET_NAME")

@dag(schedule=None, start_date=datetime(2026, 3, 26), tags=['admin'])
def upload_variables():
    @task
    def load_vars():
        s3_hook = S3Hook('yandex_s3')  # Имя connection
        file_content = s3_hook.read_key('vars/variables.json', bucket_name="{S3_BUCKET_NAME})"
        data = json.loads(file_content)
        
        for key, value in data.items():
            Variable.set(key, json.dumps(value) if isinstance(value, dict) else str(value))
        print(f"Loaded {len(data)} variables")

    load_vars()

upload_variables()