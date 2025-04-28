import os
import logging
import requests

logger = logging.getLogger(__name__)

def salvar_json(df, path):
    """
    Salva um DataFrame como um arquivo JSON no caminho especificado.

    A função cria o diretório de destino, caso ele não exista, e salva o DataFrame em formato JSON. O arquivo será
    formatado com indentação para facilitar a leitura e com codificação UTF-8 para suportar caracteres especiais.

    Args:
        df (pandas.DataFrame): O DataFrame que será convertido e salvo como um arquivo JSON.
        path (str): O caminho completo onde o arquivo JSON será salvo.

    Exceções:
        O processo de salvar o arquivo não gera exceções, mas caso haja problemas na criação do diretório ou no
        processo de gravação, o erro será registrado no log.

    Exemplos:
        >>> import pandas as pd
        >>> df = pd.DataFrame({'id': [1, 2], 'nome': ['João', 'Maria']})
        >>> salvar_json(df, 'caminho/para/o/arquivo.json')
        Arquivo salvo em: caminho/para/o/arquivo.json
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_json(path, orient="records", indent=2, force_ascii=False)
    logger.info(f"Arquivo salvo em: {path}")

def requisicao_api(url):
    """
    Realiza uma requisição GET para a URL fornecida e retorna o conteúdo JSON da resposta.

    A função tenta realizar uma requisição GET para a URL especificada. Caso a requisição seja bem-sucedida, os
    dados retornados no formato JSON são retornados como um dicionário Python. Em caso de erro na requisição, um
    log de erro é gerado e a função retorna `None`.

    Args:
        url (str): A URL da API para a qual a requisição GET será feita.

    Returns:
        dict | None: Um dicionário com os dados JSON retornados pela API, ou `None` em caso de erro.

    Exceções:
        Se a requisição falhar (por exemplo, se houver problemas de rede ou a API retornar um erro), um log de erro será gerado.
        A exceção `requests.RequestException` é capturada e o erro é logado com detalhes da falha.

    Exemplos:
        >>> url = "https://api.exemplo.com/dados"
        >>> dados = requisicao_api(url)
        >>> if dados:
        >>>     print(dados)
        {'id': 1, 'nome': 'João'}

        >>> url = "https://api.exemplo.com/erro"
        >>> dados = requisicao_api(url)
        >>> if not dados:
        >>>     print("Erro na requisição.")
        Erro na requisição: https://api.exemplo.com/erro - <detalhes do erro>
    """
    try:
        response = requests.get(url)
        response.raise_for_status()  # Levanta uma exceção para status de erro HTTP
        return response.json()
    except requests.RequestException as e:
        logger.error(f"Erro na requisição: {url} - {e}")
        return None
