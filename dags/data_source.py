"""
DAG para coleta e validação dos dados do IBGE.

Este script configura uma DAG do Airflow para coletar e validar dados de diferentes fontes do IBGE, como regiões, estados, municípios, distritos, subdistritos e dados populacionais. A DAG utiliza um fluxo de trabalho em que cada tarefa coleta os dados, valida a qualidade e os salva em arquivos JSON.


URLs de APIs:
- Regiões, Estados, Intermediárias, Imediatas, Municípios, Distritos, Subdistritos, População.
"""

from airflow.decorators import dag, task, task_group
from datetime import datetime, timedelta

from script.utils import salvar_json, requisicao_api
from script.ibge_schema import schemas
from script.validador import ValidadorDataQuality
from script.coletor import ColetorAPI

import os
import pandas as pd
import logging

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# URL base para as APIs do IBGE
IBGE_BASE_URL = "https://servicodados.ibge.gov.br/api/v1/localidades"

# Diretório de dados onde os arquivos JSON serão salvos
DATA_DIR = "/usr/local/airflow/include/ibge"

# URLs específicas para cada categoria de dados do IBGE
URLS = {
    "regioes": f"{IBGE_BASE_URL}/regioes?orderBy=nome",
    "estados": f"{IBGE_BASE_URL}/estados",
    "intermediarias": f"{IBGE_BASE_URL}/estados/{{}}/regioes-intermediarias",
    "imediatas": f"{IBGE_BASE_URL}/estados/{{}}/regioes-imediatas",
    "municipios": f"{IBGE_BASE_URL}/estados/{{}}/municipios",
    "distritos": f"{IBGE_BASE_URL}/distritos",
    "subdistritos": f"{IBGE_BASE_URL}/distritos/{{}}/subdistritos",
    "populacao": "https://servicodados.ibge.gov.br/api/v3/agregados/6579/periodos/2014%7C2015%7C2016%7C2017%7C2018%7C2019%7C2020%7C2021%7C2024/variaveis/9324?localidades=N1[all]|N2[all]|N3[all]"
}

default_args = {
    "owner": "github/lobobranco96",
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

@dag(
    start_date=datetime(2025, 4, 23),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["ibge", "validação", "etl"]
)
def dag_ibge_data_source():

    def criar_task_group(nome_contexto: str, metodo_coletor: str):
        """
        Cria um TaskGroup para coletar e validar os dados de um contexto específico do IBGE.

        Parâmetros:
        - nome_contexto (str): Nome do contexto (e.g., "regioes", "estados").
        - metodo_coletor (str): Nome do método do coletor que será chamado para coleta de dados.
        """
        @task_group(group_id=f"{nome_contexto}_task", tooltip="Pipeline de coleta e validação dos dados do IBGE", prefix_group_id=False)
        def pipeline():
            @task
            def extrair() -> pd.DataFrame:
                """
                Tarefa de extração de dados usando a API do IBGE.

                Retorna:
                - pd.DataFrame: DataFrame contendo os dados extraídos da API do IBGE.
                """
                api = ColetorAPI(urls=URLS, requisicao_api=requisicao_api, data_dir=DATA_DIR)
                metodo = getattr(api, metodo_coletor)
                return metodo()

            @task
            def validar_salvar(df: pd.DataFrame) -> str:
                """
                Tarefa de validação dos dados e salvamento em arquivo JSON.

                Parâmetros:
                - df (pd.DataFrame): DataFrame contendo os dados a serem validados.

                Retorna:
                - str: Caminho onde o arquivo foi salvo ou mensagem de erro.
                """
                tipos = schemas.get(nome_contexto)
                if not tipos:
                    raise ValueError(f"Schema não encontrado para o contexto '{nome_contexto}'")

                validador = ValidadorDataQuality(df=df, contexto=nome_contexto, tipos_esperados=tipos)
                if validador.validar():
                    path = os.path.join(DATA_DIR, f"{nome_contexto}_validado.json")
                    salvar_json(df, path)
                    return f"✅ Salvo com sucesso em: {path}"
                else:
                    raise ValueError(f"❌ Falha na validação para {nome_contexto}")

            # Execução das tarefas de extração e validação
            df = extrair()
            validar_salvar(df)

        return pipeline()

    # Chamando todos os TaskGroups para as diferentes categorias de dados do IBGE
    regioes = criar_task_group("regioes", "get_regioes")
    estados = criar_task_group("estados", "get_estados")
    intermediarias = criar_task_group("intermediarias", "get_intermediarias")
    imediatas = criar_task_group("imediatas", "get_imediatas")
    municipios = criar_task_group("municipios", "get_municipios")
    distritos = criar_task_group("distritos", "get_distritos")
    subdistritos = criar_task_group("subdistritos", "get_subdistritos_paralelo")
    populacao = criar_task_group("populacao", "get_populacao")

    # Definindo as dependências entre os task groups
    regioes >> [estados, distritos, populacao]  # As tarefas podem rodar ao mesmo tempo
    estados >> [intermediarias, imediatas, municipios]
    municipios >> subdistritos

# Instanciando a DAG
dag_instance = dag_ibge_data_source()