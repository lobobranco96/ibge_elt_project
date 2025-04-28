import pandas as pd
import logging

logger = logging.getLogger(__name__)

class ValidadorDataQuality:
    """
    Classe responsável por validar a qualidade dos dados em um DataFrame, verificando aspectos como:
    - DataFrame vazio.
    - Linhas completamente nulas.
    - Registros duplicados.
    - Tipos de dados esperados nas colunas.

    Métodos:
        validar: Executa as validações de qualidade de dados e gera logs adequados.
    """

    def __init__(self, df: pd.DataFrame, contexto: str, tipos_esperados: dict = None):
        """
        Inicializa o validador de qualidade de dados.

        Args:
            df (pandas.DataFrame): O DataFrame a ser validado.
            contexto (str): Descrição ou nome do contexto em que a validação é realizada. Usado em logs.
            tipos_esperados (dict, opcional): Dicionário que mapeia os nomes das colunas para os tipos esperados.
                Se não fornecido, a validação de tipo será ignorada.
        """
        self.df = df
        self.contexto = contexto
        self.tipos_esperados = tipos_esperados or {}

    def validar(self) -> bool:
        """
        Realiza as validações de qualidade de dados no DataFrame.

        Verifica se o DataFrame está vazio, se há linhas completamente nulas, se existem registros duplicados
        e se os tipos das colunas são os esperados.

        - Se o DataFrame estiver vazio, emite um log de advertência.
        - Se houver linhas completamente nulas, emite um log de advertência.
        - Se houver registros duplicados, emite um log de advertência.
        - Se os tipos das colunas não corresponderem aos tipos esperados, emite um log de erro.

        Returns:
            bool: Retorna `True` se todas as validações foram concluídas com sucesso, caso contrário, retorna `False`.

        Exemplos:
            >>> df = pd.DataFrame({'coluna1': [1, 2, None, 4], 'coluna2': ['a', 'b', 'c', 'd']})
            >>> validador = ValidadorDataQuality(df, 'Teste de Qualidade')
            >>> validador.validar()
            [Data Quality] ✅ Validação concluída com sucesso para Teste de Qualidade.
            True

            >>> df_invalid = pd.DataFrame({'coluna1': [1, 2, 3, 4], 'coluna2': [None, None, None, None]})
            >>> validador_invalid = ValidadorDataQuality(df_invalid, 'Teste de Qualidade com Erro')
            >>> validador_invalid.validar()
            [Data Quality] ⚠️ Linhas completamente nulas encontradas em Teste de Qualidade com Erro.
            [Data Quality] ✅ Validação concluída com sucesso para Teste de Qualidade com Erro.
            True
        """
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
