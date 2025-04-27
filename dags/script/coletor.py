import os
import pandas as pd
import requests
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


class ColetorAPI:
    def __init__(self, urls: dict, requisicao_api: str, data_dir: str):
        self.urls = urls
        self.requisicao_api = requisicao_api
        self.data_dir = data_dir

    def get_regioes(self):
        dados = self.requisicao_api(self.urls["regioes"])

        if not dados:
            return None

        df = pd.DataFrame(dados).rename(columns={
            "id": "regiao.id", "sigla": "regiao.sigla", "nome": "regiao.nome"
        })

        return df

    def get_estados(self):
      dados = self.requisicao_api(self.urls["estados"])
      if not dados:
          return None

      df = pd.DataFrame(dados).rename(columns={
          "id": "estado.id", "sigla": "estado.sigla", "nome": "estado.nome"
      })

      df_regiao = pd.json_normalize(df["regiao"]).rename(columns={"id": "regiao.id"})
      df_final = pd.concat([df.drop(columns=["regiao"]), df_regiao.drop(columns=["sigla", "nome"])], axis=1)

      return df_final

    def get_intermediarias(self):
      estados = pd.read_json(os.path.join(self.data_dir, "estados_validado.json"))["estado.id"].tolist()
      df = pd.DataFrame()
      for estado_id in estados:
          dados = self.requisicao_api(self.urls["intermediarias"].format(estado_id))
          if not dados:
              continue

          df1 = pd.DataFrame(dados).rename(columns={
              "id": "intermediaria.id", "nome": "intermediaria.nome"
          })

          df_uf = pd.json_normalize(df1["UF"]).rename(columns={
              "id": "uf.id", "sigla": "uf.sigla"
          }).drop(columns=["nome", "regiao.id", "regiao.sigla", "regiao.nome"])

          df_final = pd.concat([df1.drop(columns=["UF"]), df_uf], axis=1)
          df = pd.concat([df, df_final], ignore_index=True)

      return df

    def get_imediatas(self):
      estados = pd.read_json(os.path.join(self.data_dir, "estados_validado.json"))["estado.id"].tolist()
      df = pd.DataFrame()

      for estado_id in estados:
        dados = self.requisicao_api(self.urls["imediatas"].format(estado_id))
        if not dados:
            continue

        df1 = pd.DataFrame(dados).rename(columns={"id": "imediata.id", "nome": "imediata.nome"})
        df_reg = pd.json_normalize(df1["regiao-intermediaria"]).rename(columns={"id": "regiao-intermediaria.id"})
        df_final = pd.concat([df1.drop(columns=["regiao-intermediaria"]), df_reg.drop(columns=["nome", "UF.id", "UF.regiao.id", "UF.regiao.sigla", "UF.regiao.nome"])], axis=1)
        df = pd.concat([df, df_final], ignore_index=True)
      return df

    def get_municipios(self):
      estados = pd.read_json(os.path.join(self.data_dir, "estados_validado.json"))["estado.id"].tolist()
      df = pd.DataFrame()

      for estado_id in estados:
        dados = self.requisicao_api(self.urls["municipios"].format(estado_id))
        if not dados:
            continue

        df1 = pd.DataFrame(dados).rename(columns={"id": "municipio.id", "nome": "municipio.nome"}).drop(columns=["regiao-imediata"])
        df_microrregiao = pd.json_normalize(df1["microrregiao"]).rename(columns={"id": "microrregiao.id", "nome": "microrregiao.nome"})
        df_final = pd.concat([df1.drop(columns=["microrregiao"]), df_microrregiao.drop(columns=["mesorregiao.UF.regiao.id", "mesorregiao.UF.regiao.sigla", "mesorregiao.UF.regiao.nome"])], axis=1)
        df = pd.concat([df, df_final], ignore_index=True)

      return df

    def get_distritos(self):
        dados = self.requisicao_api(self.urls["distritos"])

        df = pd.DataFrame(dados).rename(columns={
            "id": "distrito.id",
            "nome": "distrito.nome"
        })

        df_municipio = pd.json_normalize(df["municipio"]).rename(columns={
            "id": "municipio.id",
            "nome": "municipio.nome"
        })

        df_final = pd.concat(
            [df.drop(columns=["municipio"]), df_municipio[["municipio.id", "regiao-imediata.regiao-intermediaria.UF.nome"]]],
            axis=1
        )

        return df_final

    def get_subdistritos_paralelo(self):
        distrito_ids = pd.read_json(os.path.join(self.data_dir, "distritos_validado.json"))["distrito.id"].tolist()
        urls = [self.urls["subdistritos"].format(did) for did in distrito_ids]

        resultados = []
        with ThreadPoolExecutor(max_workers=20) as executor:
            futures = {executor.submit(self.requisicao_api, url): url for url in urls}
            for future in as_completed(futures):
                data = future.result()
                if data:
                    resultados.append(data)

        # Agora transforma todos os JSONs em um único DataFrame
        df_final = pd.DataFrame()
        for dados in resultados:
            df = pd.DataFrame(dados).rename(columns={"id": "bairro.id", "nome": "bairro.nome"})
            df_distrito = pd.json_normalize(df["distrito"]).rename(columns={"id": "distrito.id", "nome": "distrito.nome"})
            df_merged = pd.concat([df.drop(columns=["distrito"]), df_distrito[["distrito.id", "municipio.nome"]]], axis=1)
            df_final = pd.concat([df_final, df_merged], ignore_index=True)

        return df_final

    def get_populacao(self):
        dados = self.requisicao_api(self.urls["populacao"])
        if not dados:
            return None
        
        df = pd.DataFrame(dados)
        df_resultados = pd.json_normalize(df['resultados'].explode().reset_index(drop=True))

        # Normalizando a lista de 'series' (cada item dentro da lista 'series' é um dicionário)
        populacao_re = pd.json_normalize(df_resultados['series'].explode().reset_index(drop=True))\
            .drop(index=[0,1,2,3,4,5])\
            .reset_index(drop=True)\
            .drop(columns=['localidade.nivel.id'])
        
        return populacao_re