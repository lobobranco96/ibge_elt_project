import pandas as pd
import logging

logger = logging.getLogger(__name__)

class ValidadorDataQuality:
    def __init__(self, df: pd.DataFrame, contexto: str, tipos_esperados: dict = None):
        self.df = df
        self.contexto = contexto
        self.tipos_esperados = tipos_esperados or {}

    def validar(self):
        if self.df.empty:
            logger.warning(f"[Data Quality] ❌ DataFrame vazio para {self.contexto}.")
            return False

        if self.df.isnull().all(axis=1).any():
            logger.warning(f"[Data Quality] ⚠️ Linhas completamente nulas encontradas em {self.contexto}.")

        if self.df.duplicated().any():
            logger.warning(f"[Data Quality] ⚠️ Registros duplicados encontrados em {self.contexto}.")

        # ✅ Verificação de tipos esperados de colunas
        for coluna, tipo_esperado in self.tipos_esperados.items():
            if coluna not in self.df.columns:
                logger.warning(f"[Data Quality] 🚫 Coluna esperada '{coluna}' ausente em {self.contexto}.")
                return False

            tipo_real = self.df[coluna].dropna().map(type).mode()[0]
            if tipo_real is not tipo_esperado:
                logger.warning(
                    f"[Data Quality] 🚫 Coluna '{coluna}' em {self.contexto} deveria ser {tipo_esperado.__name__} "
                    f"mas é {tipo_real.__name__}."
                )
                return False

        logger.info(f"[Data Quality] ✅ Validação concluída com sucesso para {self.contexto}.")
        return True
