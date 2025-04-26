import os
from pathlib import Path
from pendulum import datetime

from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from astro import sql as aql
from astro.files import File
from astro.sql.table import Table

POSTGRES_CONN_ID = "postgres_dw"
DATA_DIR = "/usr/local/airflow/include/ibge"

def file_path(file):
    return Path(DATA_DIR) / file

default_args = {
    "owner": "github/lobobranco96",
    "retries": 1,
    "retry_delay": 0
}

@dag(
    start_date=datetime(2024, 4, 20),
    schedule="@daily",
    max_active_runs=1,
    catchup=False,
    doc_md=__doc__,
    default_args=default_args,
    tags=["python", "postgres", "dbt"],
)
def local_to_postgres():

    # Função para carregar os arquivos JSON no PostgreSQL
    def load_json_to_postgres(file_name: str):
        path = file_path(file_name)
        table_name = Path(path).stem  # Remove a extensão .json
        return aql.load_file(
            input_file=File(path=path),
            output_table=Table(name=table_name, conn_id=POSTGRES_CONN_ID),
            if_exists="replace",
        )

    with TaskGroup("load_files", tooltip="Carregar arquivos para o PostgreSQL") as load_files_group:
        arquivos = [
            "estados_validado.json",
            "municipios_validado.json",
            "distritos_validado.json",
            "intermediarias_validado.json",
            "imediatas_validado.json",
            "municipios_validado.json",
            "distritos_validado.json",
            "subdistritos_validado.json"
        ]
        
        # Cria uma tarefa para cada arquivo
        for arquivo in arquivos:
            load_json_to_postgres(arquivo)

dag = local_to_postgres()