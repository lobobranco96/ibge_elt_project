{{ config(
    materialized="table"
) }}
with source_data as (
    select
        "regiao.id",
        "regiao.sigla",
        "regiao.nome"
    from {{ source('raw', 'regioes') }}
)
select * from source_data