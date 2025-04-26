# IBGE ELT Project

Este projeto implementa um pipeline de **ELT** (Extract, Load, Transform) utilizando a **API do IBGE Brasil** como fonte de dados. O fluxo de dados é orquestrado pelo **Apache Airflow** e utiliza ferramentas como **PostgreSQL** e **DBT** para a transformação dos dados, aplicando um modelo dimensional **Snowflake** para facilitar consultas analíticas.

## Objetivo

O objetivo deste projeto é automatizar a extração, carga e transformação de dados geográficos do Brasil, como informações sobre estados, municípios e etc, utilizando tecnologias modernas e escaláveis. O pipeline integra e organiza esses dados para análises subsequentes, tornando-os prontos para uso em ferramentas de BI ou análises customizadas.

## Fluxo de Trabalho

O pipeline segue o modelo **ELT** dividido em três etapas principais:

### 1. **Extract (Extração)**
- A extração de dados é feita via **API do IBGE**, que fornece informações sobre regioes, estados, intermediarios, imediatos, municípios, distritos e subdistritos(bairros) do Brasil.
- Os dados extraídos incluem:
  - Dados de regioes (regiao.id, regiao.sigla (SE), etc.)
  - Dados de estados (estado.id, estado.nome, estado.sigla('RJ'), etc.)
  - Dados de intermediarios (intermediario.id, intermediario.nome('Rio de Janeiro'), uf.id, etc.)
  - Dados de imediatos (imediata.id, imediata.nome, uf.sigla, etc.)
  - Dados de municípios (municipio.id, municipio.nome('Angra dos Reis'), microregiao.nome('Baía da Ilha Grande'), etc)
  - Dados de distritos (distrito.id, distrito.nome, regiao-imediata.regiao-intermediaria.UF.nome, etc)
  - Dados de subdistritos (bairro.id, bairro.nome('Penha'), municipio.nome, etc)
- Os dados sao armazenados em um diretorio /include em formato json
### 2. **Load (Carregamento)**
- Após a extração, os dados são carregados em um banco de dados **PostgreSQL** local.
- O carregamento é feito utilizando a ferramenta **load_file** do **Astronomer** para garantir um processo eficiente e sem erros.

### 3. **Transform (Transformação)**
- A transformação dos dados é realizada utilizando **DBT** (Data Build Tool), aplicando um modelo **Snowflake**.
- O modelo Snowflake organiza os dados em tabelas de fato e dimensões, o que facilita análises rápidas e eficientes.

### 4. **Orquestração com Apache Airflow**
- O **Apache Airflow** é usado para orquestrar todas as tarefas do pipeline, desde a extração dos dados da API até a execução das transformações no DBT.
- O Airflow garante a automação e a execução sequencial das tarefas, com monitoramento e alertas.

## Tecnologias Utilizadas

- **Airflow**: Orquestração das tarefas de ETL.
- **PostgreSQL**: Banco de dados relacional para armazenamento temporário dos dados.
- **DBT**: Ferramenta para transformação de dados e modelagem dimensional.
- **API do IBGE**: Fonte dos dados geográficos e demográficos do Brasil.
- **Astronomer**: Plataforma para execução e gerenciamento do Airflow.
- **Python**: Linguagem de programação para automação das tarefas de extração e carregamento de dados.

## Estrutura do Repositório

A estrutura do repositório é organizada da seguinte forma:

