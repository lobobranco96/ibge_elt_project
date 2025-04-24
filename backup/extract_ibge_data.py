import logging
from script.coletor import ColetorAPI
import os

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from pendulum import datetime
from datetime import timedelta

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


default_args = {
    "owner": "github/lobobranco96",
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

@dag(
    start_date=datetime(2025, 4, 23),
    schedule="@daily",
    max_active_runs=1,
    catchup=False,
    doc_md=__doc__,
    default_args=default_args,
    tags=["python", "astronomer"],
)
def ibge_data_source():

    init = EmptyOperator(task_id="inicio")
    finish = EmptyOperator(task_id="fim_pipeline")

    @task(task_id="iniciar_extracao_regioes")
    def coletar_regioes():
        try:
            api = ColetorAPI()
            regioes = api.get_regioes()
            logger.info(f"Extração concluída. Arquivo armazenado em {regioes[1]}")
        except Exception as e:
            logger.error(f"Erro ao coletar dados da API: {e}")

    @task(task_id="iniciar_extracao_estados")
    def coletar_estados():
        try:
            api = ColetorAPI()
            estados = api.get_estados()
            logger.info(f"Extração concluída. Arquivo armazenado em {estados[1]}")
        except Exception as e:
            logger.error(f"Erro ao coletar dados da API: {e}")
            
    @task(task_id="iniciar_extracao_intermediarias")
    def coletar_intermediarias():
        try:
            api = ColetorAPI()
            intermediaria = api.get_intermediarias()
            logger.info(f"Extração concluída. Arquivo armazenado em {intermediaria[1]}")
        except Exception as e:
            logger.error(f"Erro ao coletar dados da API: {e}")

    @task(task_id="iniciar_extracao_imediatas")
    def coletar_imediatas():
        try:
            api = ColetorAPI()
            imediatas = api.get_imediatas()
            logger.info(f"Extração concluída. Arquivo armazenado em {imediatas[1]}")
        except Exception as e:
            logger.error(f"Erro ao coletar dados da API: {e}")

    @task(task_id="iniciar_extracao_municipios")
    def coletar_municipios():
        api = ColetorAPI()
        municipios = api.get_municipios()
        logger.info(f"Extração concluída. ")

    @task(task_id="iniciar_extracao_distritos")
    def coletar_distritos():
        api = ColetorAPI()
        distritos = api.get_distritos()
        logger.info(f"Extração concluída.")

    @task(task_id="iniciar_extracao_bairros")
    def coletar_bairros():
                
        arquivos = os.listdir("/usr/local/airflow/include/ibge/distritos/")
        api = ColetorAPI()
        bairros = api.get_subdistritos(arquivos)
        logger.info(f"Extração concluída.")

    task1 = coletar_regioes()
    task2 = coletar_estados()
    task3 = coletar_intermediarias()
    task4 = coletar_imediatas()
    task5 = coletar_municipios()
    task6 = coletar_distritos()
    task7 = coletar_bairros()

    init >> task1 >> task2 >> task3 >> task4 >> task5 >> task6 >> task7 >> finish

dag_instance = ibge_data_source()