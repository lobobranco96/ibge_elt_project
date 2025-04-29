{{ config(
    materialized="table"
) }}

with source_data as (
    select
        "distrito.id",
        "distrito.nome",
        "municipio.id",
        "regiao-imediata.regiao-intermediaria.UF.nome"
    from {{ source('raw', 'distritos') }}
)
select * from source_data
