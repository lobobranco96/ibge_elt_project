{{ config(
    materialized="table"
) }}

with source_data as (
    select
        "municipio.id",
        "municipio.nome",
        "microrregiao.id",
        "microrregiao.nome",
        "mesorregiao.id",
        "mesorregiao.nome",
        "mesorregiao.UF.id",
        "mesorregiao.UF.sigla",
        "mesorregiao.UF.nome"
    from {{ source('raw', 'municipios') }}
)
select * from source_data
