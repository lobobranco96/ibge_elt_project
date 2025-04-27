from pathlib import Path
from airflow.decorators import dag
from pendulum import datetime
from cosmos import DbtTaskGroup, ProfileConfig, ProjectConfig

DBT_PATH = "/usr/local/airflow/dags/dbt"
DBT_PROFILE = "dbt_project"
DBT_TARGETS = "dev"

DATA_DIR = "/usr/local/airflow/include/ibge"

profile_config = ProfileConfig(
    profile_name=DBT_PROFILE,
    target_name=DBT_TARGETS,
    profiles_yml_filepath=Path(f'{DBT_PATH}/profiles.yml')
)

# Models dbt
project_config = ProjectConfig(
    dbt_project_path=DBT_PATH,
    models_relative_path="models"
)

default_args = {
    "owner": "github/lobobranco96",
    "retries": 1,
    "retry_delay": 0
}

# Define the basic parameters of the DAG, like schedule and start_date
@dag(
    start_date=datetime(2024, 4, 27),
    schedule="@daily",
    max_active_runs=1,
    catchup=False,
    doc_md=__doc__,
    default_args=default_args,
    tags=["python", "postgres", "dbt"],
)
def dbt_teste():

    dbt_running_models = DbtTaskGroup(
        group_id="dbt_running_models",
        project_config=project_config,
        profile_config=profile_config,
        default_args={"retries": 2},
        )
    
    dbt_running_models
dbt_dag = dbt_teste()
