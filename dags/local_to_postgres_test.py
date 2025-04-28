"""
DAG para carregar arquivos JSON no PostgreSQL usando o Astro e Airflow.

Este script configura e executa uma DAG do Airflow para carregar arquivos JSON do diretório local para tabelas PostgreSQL. A DAG utiliza a biblioteca Astro para manipulação de arquivos e SQL, e o PostgreSQL como destino para os dados.

Diretórios de interesse:
- DATA_DIR: Caminho para o diretório que contém os arquivos JSON a serem carregados.

Conexões do Airflow:
- POSTGRES_CONN_ID: ID da conexão do Airflow para conectar ao banco de dados PostgreSQL.
"""

import os
from pathlib import Path
from pendulum import datetime

from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from astro import sql as aql
from astro.files import File
from astro.sql.table import Table

# ID da conexão do PostgreSQL
POSTGRES_CONN_ID = "postgres_dw"
# Caminho para o diretório onde os arquivos JSON estão localizados
DATA_DIR = "/usr/local/airflow/include/ibge"

# Função para gerar o caminho completo do arquivo
def file_path(file):
    """
    Retorna o caminho completo de um arquivo dentro do diretório DATA_DIR.

    Parâmetros:
    - file (str): Nome do arquivo.

    Retorna:
    - Path: Caminho completo do arquivo.
    """
    return Path(DATA_DIR) / file

# Argumentos padrão da DAG
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
def local_to_postgres():
    # Função para carregar arquivos JSON no PostgreSQL
    def load_json_to_postgres(file_name: str):
        """
        Carrega um arquivo JSON no PostgreSQL.

        Parâmetros:
        - file_name (str): Nome do arquivo JSON a ser carregado.

        Retorna:
        - Tarefa do Airflow para carregar o arquivo.
        """
        # Obtém o caminho completo do arquivo
        path = file_path(file_name)
        # Nome da tabela no PostgreSQL (removendo a parte "_validado" do nome do arquivo)
        table_name = Path(path).stem.replace("_validado", "")  # Remove a extensão .json
        # Carrega o arquivo JSON para o PostgreSQL
        return aql.load_file(
            input_file=File(path=str(path)),
            output_table=Table(
                name=table_name,
                conn_id=POSTGRES_CONN_ID),  # Conexão com o PostgreSQL
            if_exists="replace",  # Substitui a tabela se ela já existir
        )

    # Agrupamento de tarefas para carregar arquivos para o PostgreSQL
    with TaskGroup("load_files", tooltip="Carregar arquivos para o PostgreSQL") as load_files_group:
        arquivos = [
            "regioes_validado.json",
            "populacao_validado.json",
            "estados_validado.json",
            "intermediarias_validado.json",
            "imediatas_validado.json",
            "municipios_validado.json",
            "distritos_validado.json",
            "subdistritos_validado.json"
        ]
        
        # Cria uma tarefa para cada arquivo JSON
        for arquivo in arquivos:
            load_json_to_postgres(arquivo)

# Instancia a DAG para o Airflow
dag = local_to_postgres()