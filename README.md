# IBGE ELT Project

Este projeto implementa um pipeline de **ELT** (Extract, Load, Transform) utilizando a **API do IBGE Brasil** como fonte de dados. O fluxo de dados é orquestrado pelo **Apache Airflow** e utiliza ferramentas como **PostgreSQL** e **DBT** para a transformação dos dados, aplicando um modelo dimensional **Snowflake** para facilitar consultas analíticas.

## Objetivo

O objetivo deste projeto é automatizar a extração, carga e transformação de dados geográficos do Brasil, como informações sobre estados, municípios e etc, utilizando tecnologias modernas e escaláveis. O pipeline integra e organiza esses dados para análises subsequentes, tornando-os prontos para uso em ferramentas de BI ou análises customizadas.

## Fluxo de Trabalho

O pipeline segue o modelo **ELT** dividido em três etapas principais:

### 1. **Extract (Extração) data_source.py DAG**
- A extração de dados é feita via **API do IBGE**, que fornece informações sobre regioes, estados, intermediarios, imediatos, municípios, distritos e subdistritos(bairros) do Brasil.
- Os dados extraídos incluem:
  - Dados de regioes (regiao.id, regiao.sigla (SE), etc.)
  - Dados de estados (estado.id, estado.nome, estado.sigla('RJ'), etc.)
  - Dados de intermediarios (intermediario.id, intermediario.nome('Rio de Janeiro'), uf.id, etc.)
  - Dados de imediatos (imediata.id, imediata.nome, uf.sigla, etc.)
  - Dados de municípios (municipio.id, municipio.nome('Angra dos Reis'), microregiao.nome('Baía da Ilha Grande'), etc)
  - Dados de distritos (distrito.id, distrito.nome, regiao-imediata.regiao-intermediaria.UF.nome, etc)
  - Dados de subdistritos (bairro.id, bairro.nome('Penha'), municipio.nome, etc)
- Antes de armazenar os dados, é feita um data quality check para a segurar um schema predefinido para assim armazenar os dados em um diretorio /include em formato json.
  
### 2. **Load (Carregamento) elt_pipeline.py DAG**
- Inicio da dag elt_pipeline com as seguintes tasks:
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
- **API do IBGE**: Fonte dos dados geográficos do Brasil.
- **Astronomer**: Plataforma para execução e gerenciamento do Airflow.
- **Python**: Linguagem de programação para automação das tarefas de extração e carregamento de dados.

## Estrutura do Repositório

A estrutura do repositório é organizada da seguinte forma:

```bash
ibge_elt_project/
├── dags/                          # Arquivos responsáveis pelas DAGs do Airflow
│   ├── scripts/                   # Scripts auxiliares para o pipeline
│   │   ├── coletor.py             # Script para coletar dados da API do IBGE
│   │   ├── utils.py               # Funções auxiliares para processamento
│   │   ├── ibge_schema.py         # Definição do esquema dos dados do IBGE
│   │   └── validador.py           # Funções para validação dos dados
│   ├── data_source.py             # Tarefa para extrair dados da API do IBGE
│   └── elt_pipeline.py            # Tarefa para carregar dados no PostgreSQL, rodar as transformações DBT
├── include/                         
│   └── ibge/                   
|   │   ├── distritos_validado.json    # Arquivo JSON com dados de distritos validados
|   │   ├── estados_validado.json      # Arquivo JSON com dados de estados validados
|   │   ├── imediatas_validado.json    # Arquivo JSON com dados de imediatas validados
|   │   ├── intermediarias_validado.json # Arquivo JSON com dados de intermediárias validados
|   │   ├── municipios_validado.json   # Arquivo JSON com dados de municípios validados
|   │   ├── regioes_validado.json      # Arquivo JSON com dados de regiões validadas
|   │   └── subdistritos_validado.json # Arquivo JSON com dados de subdistritos validados        
│   |
├── dbt/                           # Arquivos de configuração e modelos DBT
│   ├── dbt_project.yml            # Configuração do projeto DBT
│   ├── profiles.yml               # Arquivo de configurações do DBT
│   └── models/                    # Modelos DBT para transformação de dados
│       ├── source.yml             # Definições das fontes de dados
│       ├── staging/               # Modelos de staging
│       └── marts/                 # Modelos de mart
├── docker-compose.override.yml    # imagem postgres para o nosso dw
├── Dockerfile                     # astronomer
├── images/                        # Pasta para armazenar imagens
├── requirements.txt               # Dependências do projeto
└── README.md                      # Documentação do projeto
```

```bash
[ fato_populacao ] 
    (id_estado PK, id_estado FK)

        |
        |  (N:1)
        v

[ dim_estado ]
    (id_estado PK, id_regiao FK, id_intermediario FK)

        |                               
        |  (N:1)                         (N:1)
        v                                v

[ dim_regiao ]                      [ dim_intermediario ]
    (id_regiao PK)                      (id_intermediario PK)

                                           |
                                           | (N:1)
                                           v

                                 [ dim_imediata ]
                                     (id_imediata PK, id_municipio FK)

                                           |
                                           | (N:1)
                                           v

                                 [ dim_municipio ]
                                     (id_municipio PK)

                                           |
                                           | (1:N)
                                           v

                                 [ dim_distrito ]
                                     (id_distrito PK, id_municipio FK)

                                           |
                                           | (1:N)
                                           v

                                 [ dim_subdistrito ]
                                     (id_subdistrito PK, id_distrito FK)

```

## Relancionamento
```bash
1. Hierarquia das Dimensões:
dim_regiao → dim_estado (via id_regiao)

dim_estado → dim_intermediaria (via id_uf)

dim_intermediaria → dim_imediata (via id_intermediaria)

dim_imediata → dim_municipio (via id_mesorregiao, dependendo da estrutura de intermediárias)

dim_municipio → dim_distrito (via id_municipio)

dim_distrito → dim_subdistrito (via id_distrito)

2. Fato População:
O fato_populacao pode se conectar a dim_municipio, dim_estado, ou dim_regiao, dependendo da granularidade de "localidade".
```

## Como Rodar o Projeto

### Pré-requisitos

Antes de rodar o projeto, certifique-se de ter as seguintes ferramentas instaladas:

- **Python 3.8+**
- **Astro CLI** para exectar o astro dev start
- **PostgreSQL**
- **DBT**

### Passos para Execução

1. **Clone o repositório:**

   ```bash
   git clone https://github.com/lobobranco96/ibge_elt_project.git
   cd ibge_elt_project
   ```
