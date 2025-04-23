import unicodedata
import os
import pandas as pd
import requests
import logging


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


class ColetorAPI:
  def __init__(self):
    self.base_url_regiao = "https://servicodados.ibge.gov.br/api/v1/localidades/regioes?orderBy=nome"
    self.base_url_estados = "https://servicodados.ibge.gov.br/api/v1/localidades/estados"
    self.base_url_intermediaria = "https://servicodados.ibge.gov.br/api/v1/localidades/estados/{}/regioes-intermediarias"
    self.base_url_imediatas = "https://servicodados.ibge.gov.br/api/v1/localidades/estados/{}/regioes-imediatas"
    self.base_url_municipios = "https://servicodados.ibge.gov.br/api/v1/localidades/estados/{}/municipios"
    self.base_url_distritos = "https://servicodados.ibge.gov.br/api/v1/localidades/distritos"
    self.base_url_subdistritos = "https://servicodados.ibge.gov.br/api/v1/localidades/distritos/{}/subdistritos"

  def get_regioes(self):
    url = self.base_url_regiao

    try:
      response = requests.get(url)
      regioes = response.json()

      df = pd.DataFrame(regioes)\
      .rename(columns={"id": "regiao.id","sigla": "regiao.sigla" ,"nome": "regiao.nome"})
      path = "/usr/local/airflow/include/ibge/regioes.json"

      logger.info(f"Extração concluída.")
      return df.to_json(path,orient="records", indent=2, force_ascii=False), path
  
    except requests.exceptions.RequestException as e:
      logging.error(f"Erro de requisição ao acessar {url}: {e}")
      return None

  def get_estados(self):
    url = self.base_url_estados

    try:
      response = requests.get(self.base_url_estados)
      estados = response.json()

      df = pd.DataFrame(estados)\
      .rename(columns={"id": "estado.id", "sigla": "estado.sigla", "nome": "estado.nome"})

      df_normalizado = pd.json_normalize(df["regiao"]).rename(columns={"id": "regiao.id"})\
      .drop(columns=["sigla", "nome"])

      df_final = pd.concat([df.drop(columns=["regiao"]), df_normalizado], axis=1)

      path = "/usr/local/airflow/include/ibge/estados.json"
      logger.info(f"Extração concluída.")
      return df_final.to_json(path,orient="records", indent=2, force_ascii=False), path
    
    except requests.exceptions.RequestException as e:
      logging.error(f"Erro de requisição ao acessar {url}: {e}")
      return None
    

  def get_intermediarias(self):
    estados_id = pd.read_json("/usr/local/airflow/include/ibge/estados.json")['estado.id'].tolist()

    for id in estados_id:
      url = self.base_url_intermediaria.format(id)

      df = pd.DataFrame(requests.get(url).json())\
      .rename(columns={"id": "intermediaria.id", "sigla": "intermediaria.sigla", "nome": "intermediaria.nome"})

      df_normalizado = pd.json_normalize(df["UF"])\
      .rename(columns={"id": "uf.id", "sigla": "uf.sigla"})\
      .drop(columns=["nome", "regiao.id", "regiao.sigla", "regiao.nome"])

      df_final = pd.concat([df.drop(columns=["UF"]), df_normalizado], axis=1)

      sigla = df_final["uf.sigla"].iloc[0]
      path = f"/usr/local/airflow/include/ibge/intermediarias/{sigla}.json"
      df_final.to_json(path,orient="records", indent=2, force_ascii=False), path
      logger.info(f"Extração concluída para o estado: {sigla}")

  def get_imediatas(self):
     estados_id = pd.read_json("/usr/local/airflow/include/ibge/estados.json")['estado.id'].tolist()

     for id in estados_id:
       url = self.base_url_imediatas.format(id)

       df = pd.DataFrame(requests.get(url).json())\
       .rename(columns={"id": "imediata.id", "nome": "imediata.nome"})

       df_normalizado = pd.json_normalize(df["regiao-intermediaria"])\
       .rename(columns={"id": "regiao-intermediaria.id"})\
       .drop(columns=["nome", "UF.id", "UF.regiao.id", "UF.regiao.sigla", "UF.regiao.nome"])

       df_final = pd.concat([df.drop(columns=["regiao-intermediaria"]), df_normalizado], axis=1)

       uf_nome = unicodedata.normalize('NFKD', str(df_final["UF.nome"].iloc[0]))\
            .encode('ascii', 'ignore')\
            .decode('utf-8')\
            .replace(" ", "_")\
            .lower()
       path = f"/usr/local/airflow/include/ibge/imediatas/{uf_nome}.json"
       df_final.to_json(path, orient="records", indent=2, force_ascii=False), path
       logger.info(f"Extração concluída para o estado: {uf_nome}")


  def get_municipios(self):
    estados_id = pd.read_json("/usr/local/airflow/include/ibge/estados.json")['estado.id'].tolist()
    for id in estados_id:
      url = self.base_url_municipios.format(id)
      df = pd.DataFrame(requests.get(url).json())\
      .rename(columns={"id": "municipio.id", "nome": "municipio.nome"}).drop(columns=["regiao-imediata"])

      df_normalizado = pd.json_normalize(df["microrregiao"])\
      .rename(columns={"id": "microrregiao.id", "nome": "microrregiao.nome"})\
      .drop(columns=["mesorregiao.UF.regiao.id", "mesorregiao.UF.regiao.sigla", "mesorregiao.UF.regiao.nome"])

      df_final = pd.concat([df.drop(columns=["microrregiao"]), df_normalizado], axis=1)
      uf_nome = unicodedata.normalize('NFKD', str(df_final["mesorregiao.UF.nome"].iloc[0]))\
            .encode('ascii', 'ignore')\
            .decode('utf-8')\
            .replace(" ", "_")\
            .lower()
      path = f"/usr/local/airflow/include/ibge/municipios/{uf_nome}.json"
      df_final.to_json(path, orient="records", indent=2, force_ascii=False)
      logger.info(f"Extração concluída para o estado: {uf_nome}")

  def get_distritos(self):
    municipios = os.listdir("/usr/local/airflow/include/ibge/municipios/")
    for nome_arquivo in municipios:
      municipio = pd.read_json(f"/usr/local/airflow/include/ibge/municipios/{nome_arquivo}")['municipio.id'].to_list()

      url = self.base_url_distritos
      df = pd.DataFrame(requests.get(url).json())\
      .rename(columns={"id": "distrito.id", "nome": "distrito.nome"})

      df_normalizado = pd.json_normalize(df["municipio"])\
      .rename(columns={"id": "municipio.id", "nome": "municipio.nome"})

      df_final = pd.concat([df.drop(columns=["municipio"]), df_normalizado[["municipio.id", "regiao-imediata.regiao-intermediaria.UF.nome"]]], axis=1)

      df_final = df_final[df_final["municipio.id"].isin(municipio)]

      uf_nome = unicodedata.normalize('NFKD', str(df_final["regiao-imediata.regiao-intermediaria.UF.nome"].iloc[0]))\
              .encode('ascii', 'ignore')\
              .decode('utf-8')\
              .replace(" ", "_")\
              .lower()

      path = f"/usr/local/airflow/include/ibge/distritos/{uf_nome}.json"
      df_final.to_json(path, orient="records", indent=2, force_ascii=False)
      logger.info(f"Extração concluída para o estado: {uf_nome}")

  def get_subdistritos(self, arquivos: list):

    for arquivo in arquivos:
      distrito = pd.read_json(f"/usr/local/airflow/include/ibge/distritos/{arquivo}")['distrito.id'].to_list()
      # DataFrame para consolidar todos os dados
      df_consolidado = pd.DataFrame()

      for distrito_id in distrito:
          try:
            url = self.base_url_subdistritos.format(distrito_id)
            response = requests.get(url)
            response.raise_for_status()

            df = pd.DataFrame(response.json())\
                .rename(columns={"id": "bairro.id", "nome": "bairro.nome"})

            df_normalizado = pd.json_normalize(df["distrito"])\
                .rename(columns={"id": "distrito.id", "nome": "distrito.nome"})

            df_final = pd.concat([df.drop(columns=["distrito"]),df_normalizado[["distrito.id", "municipio.nome"]]], axis=1)
            df_consolidado = pd.concat([df_consolidado, df_final], ignore_index=True)
          except Exception as e:
                print(f"Erro ao processar distrito {distrito_id}: {e}")
                continue
          
      path = f"/usr/local/airflow/include/ibge/subdistritos/{arquivo}"
      df_consolidado.to_json(path, orient="records", indent=2, force_ascii=False)
      logger.info(f"Extração concluída para o estado: {arquivo.replace(".json", "")}")