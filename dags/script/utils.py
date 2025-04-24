import os
import logging
import requests

logger = logging.getLogger(__name__)

def salvar_json(df, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_json(path, orient="records", indent=2, force_ascii=False)
    logger.info(f"Arquivo salvo em: {path}")

def requisicao_api(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logger.error(f"Erro na requisição: {url} - {e}")
        return None