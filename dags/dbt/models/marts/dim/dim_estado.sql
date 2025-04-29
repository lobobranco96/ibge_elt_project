{{ config(
    materialized="view"
) }}

with base as (
    select
        "estado.id" as id_estado,
        "estado.sigla" as sigla_estado,
        "estado.nome" as nome_estado,
        "regiao.id" as id_regiao
    from {{ ref('stg_estados') }}
)

select
    *
from base
