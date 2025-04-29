{{ config(
    materialized="table"
) }}

with source_data as (
    select
        "imediata.id",
        "imediata.nome",
        "regiao-intermediaria.id",
        "UF.sigla",
        "UF.nome"
    from {{ source('raw', 'imediatas') }}
)
select * from source_data
