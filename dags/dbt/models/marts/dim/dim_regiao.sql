{{ config(
    materialized="view"
) }}

with base as (
    select
        "regiao.id" as id_regiao,
        "regiao.sigla" as sigla_regiao,
        "regiao.nome" as nome_regiao
    from {{ ref('stg_regioes') }}
)

select
    *
from base
