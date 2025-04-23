import logging
from script.coletor import ColetorAPI

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
def ibge_data_extract():

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


    task1 = coletar_regioes()

    init >> task1 >> finish

dag_instance = ibge_data_extract()