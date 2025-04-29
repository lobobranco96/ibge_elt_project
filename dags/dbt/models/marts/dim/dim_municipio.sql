{{ config(
    materialized="view"
) }}

with base as (
    select
        "municipio.id" as id_municipio,
        "municipio.nome" as nome_municipio,
        "microrregiao.id" as id_microrregiao,
        "microrregiao.nome" as nome_microrregiao,
        "mesorregiao.id" as id_mesorregiao,
        "mesorregiao.nome" as nome_mesorregiao,
        "mesorregiao.UF.id" as id_uf,
        "mesorregiao.UF.sigla" as sigla_uf,
        "mesorregiao.UF.nome" as nome_uf
    from {{ ref('stg_municipios') }}
)

select
    id_municipio,
    nome_municipio,
    id_microrregiao,
    nome_microrregiao,
    id_mesorregiao,
    nome_mesorregiao,
    id_uf,
    sigla_uf,
    nome_uf
from base
