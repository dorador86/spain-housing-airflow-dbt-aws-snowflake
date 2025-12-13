from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

# Path to the dbt project inside the container
DBT_PROJECT_DIR = "/opt/airflow/dbt"

with DAG(
    dag_id="dbt_transformation_dag",
    schedule="30 2 * * 1",  # Run Mondays at 02:30 (After data ingestion)
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dbt", "transformation", "snowflake"],
) as dag:

    # 1. Install dbt packages (dbt_utils, etc.)
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt deps --profiles-dir .",
    )

    # 2. Run dbt models
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --profiles-dir .",
    )

    # 3. Test data quality
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test --profiles-dir .",
    )

    # 4. Generate documentation
    dbt_docs = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt docs generate --profiles-dir .",
    )

    dbt_deps >> dbt_run >> dbt_test >> dbt_docs
