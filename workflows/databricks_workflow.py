# ── Databricks Workflow Definition ─────────────────────────────────────────────
# Uses Databricks REST API to programmatically create a Workflow (Job)
# that orchestrates the full pipeline.
# Run once in a Databricks notebook to register the job.
# ──────────────────────────────────────────────────────────────────────────────

import requests

# ── Configuration ──────────────────────────────────────────────────────────────
DATABRICKS_HOST = "https://<your-databricks-workspace>.azuredatabricks.net"
DATABRICKS_TOKEN = "<your-personal-access-token>"

HEADERS = {
    "Authorization": f"Bearer {DATABRICKS_TOKEN}",
    "Content-Type": "application/json"
}

# ── Workflow Definition ────────────────────────────────────────────────────────
workflow_payload = {
    "name": "India Warehouse Digital Twin Pipeline",
    "tags": {"project": "warehouse-digital-twin", "owner": "rahil"},
    "schedule": {
        "quartz_cron_expression": "0 0 6 * * ?",
        "timezone_id": "Asia/Kolkata"
    },
    "email_notifications": {
        "on_failure": ["samananirahil@gmail.com"]
    },
    "tasks": [
        {
            "task_key": "generate_and_upload_data",
            "description": "Generate synthetic warehouse data and upload to S3",
            "notebook_task": {
                "notebook_path": "/Repos/rahil/india-warehouse-digital-twin/data_generator/generate_data",
                "source": "GIT"
            },
            "new_cluster": {
                "spark_version": "14.3.x-scala2.12",
                "node_type_id": "i3.xlarge",
                "num_workers": 1
            },
            "max_retries": 3,
            "min_retry_interval_millis": 300000
        },
        {
            "task_key": "run_dlt_pipeline",
            "description": "Run Delta Live Tables Bronze → Silver → Gold pipeline",
            "depends_on": [{"task_key": "generate_and_upload_data"}],
            "pipeline_task": {
                "pipeline_id": "<your-dlt-pipeline-id>"
            },
            "max_retries": 2,
            "min_retry_interval_millis": 300000
        },
        {
            "task_key": "run_sql_analytics",
            "description": "Run Databricks SQL analytical queries on Gold layer",
            "depends_on": [{"task_key": "run_dlt_pipeline"}],
            "notebook_task": {
                "notebook_path": "/Repos/rahil/india-warehouse-digital-twin/sql/analytical_queries",
                "source": "GIT"
            },
            "new_cluster": {
                "spark_version": "14.3.x-scala2.12",
                "node_type_id": "i3.xlarge",
                "num_workers": 1
            },
            "max_retries": 2,
            "min_retry_interval_millis": 300000
        }
    ]
}

# ── Create Workflow via REST API ───────────────────────────────────────────────
def create_workflow():
    url = f"{DATABRICKS_HOST}/api/2.1/jobs/create"
    response = requests.post(url, headers=HEADERS, json=workflow_payload)

    if response.status_code == 200:
        job_id = response.json().get("job_id")
        print(f"Workflow created successfully. Job ID: {job_id}")
    else:
        print(f"Failed: {response.status_code}")
        print(response.json())

if __name__ == "__main__":
    create_workflow()