from cosmos import DbtTaskGroup, ProfileConfig, ProjectConfig

from pathlib import Path
from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from astro import sql as aql
from astro.files import File
from astro.sql.table import Table
from script.ibge_schema import schemas
from pendulum import datetime
import os

POSTGRES_CONN_ID = "postgres_dw"

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
    start_date=datetime(2024, 4, 20),
    schedule="@daily",
    max_active_runs=1,
    catchup=False,
    doc_md=__doc__,
    default_args=default_args,
    tags=["python", "postgres", "dbt"],
)
def elt_pipeline():

    @task
    def list_json_files():
        # Retorna uma lista de arquivos JSON na pasta
        return [f for f in os.listdir(DATA_DIR) if f.endswith(".json")]

    @task
    def load_json_to_postgres(filename: str):
        # Cria o caminho completo para o arquivo
        file_path = os.path.join(DATA_DIR, filename)
        # Carrega o arquivo JSON para a tabela 'unificada_table' no PostgreSQL
        return aql.load_file(
            input_file=File(path=file_path[0]),   # Caminho do arquivo
            output_table=Table(name="unificada_table", conn_id=POSTGRES_CONN_ID),  # Tabela no PostgreSQL
            if_exists="replace",  # Substitui os dados na tabela caso ela já exista
        )

    # Recupera a lista de arquivos JSON de maneira síncrona
    json_files = list_json_files()

    # Expande a tarefa para cada arquivo JSON
    load_json_to_postgres(filename=json_files)

elt_pipeline_dag = elt_pipeline()


    #dbt_running_models = DbtTaskGroup(
     #   group_id="dbt_running_models",
      #  project_config=project_config,
       # profile_config=profile_config,
        #default_args={"retries": 2},
    #)

#    init >> #dbt_running_models >> finish
#elt_pipeline()