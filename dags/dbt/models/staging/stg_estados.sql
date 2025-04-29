{{ config(
    materialized="table"
) }}

with source_data as (
    select
        "estado.id",
        "estado.sigla",
        "estado.nome",
        "regiao.id"
    from {{ source('raw', 'estados') }}
)
select * from source_data
