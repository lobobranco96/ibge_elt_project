"""

Diretórios de interesse:
- DBT_PATH: Caminho para o diretório DBT, onde o projeto DBT está localizado.
- DATA_DIR: Caminho para o diretório de dados do IBGE.

Configuração de DBT:
- DBT_PROFILE: Nome do perfil de configuração do DBT.
- DBT_TARGETS: Target de execução do DBT (dev, prod, etc.).
"""

from pathlib import Path
from airflow.decorators import dag
from pendulum import datetime
from cosmos import DbtTaskGroup, ProfileConfig, ProjectConfig

# Caminho para o diretório do DBT
DBT_PATH = "/usr/local/airflow/dags/dbt"
# Nome do perfil do DBT
DBT_PROFILE = "dbt_project"
# Target de execução do DBT (exemplo: 'dev')
DBT_TARGETS = "dev"

# Caminho para o diretório de dados
DATA_DIR = "/usr/local/airflow/include/ibge"

# Configuração do perfil DBT
profile_config = ProfileConfig(
    profile_name=DBT_PROFILE,
    target_name=DBT_TARGETS,
    profiles_yml_filepath=Path(f'{DBT_PATH}/profiles.yml')
)

# Configuração do projeto DBT
project_config = ProjectConfig(
    dbt_project_path=DBT_PATH,
    models_relative_path="models"  # Caminho relativo para os modelos DBT
)

# Parâmetros padrão do Airflow para a DAG
default_args = {
    "owner": "github/lobobranco96",  # Dono da DAG
    "retries": 1,  # Número de tentativas em caso de falha
    "retry_delay": 0  # Tempo de espera entre as tentativas
}

@dag(
    start_date=datetime(2024, 4, 27),
    schedule="@daily",
    max_active_runs=1,
    catchup=False,
    doc_md=__doc__,
    default_args=default_args,
    tags=["python", "postgres", "dbt"],
)
def dbt_dag():
    """
    Função principal da DAG para executar os modelos DBT.

    A DAG orquestra a execução dos modelos de transformação de dados utilizando DBT.
    """
    # Tarefa do Cosmos para rodar os modelos DBT
    dbt_running_models = DbtTaskGroup(
        group_id="dbt_running_models",
        project_config=project_config,
        profile_config=profile_config,
        default_args={"retries": 2},  # Número de tentativas para a tarefa de DBT
    )
    
    dbt_running_models  # Executa o grupo de tarefas DBT

# Instancia a DAG para o Airflow
dbt_dag = dbt_dag()
