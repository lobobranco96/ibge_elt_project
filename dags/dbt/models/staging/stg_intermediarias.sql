with source_data as (
    select
        "intermediaria.id",
        "intermediaria.nome",
        "uf.id",
        "uf.sigla"
    from {{ source('raw', 'intermediarias') }}
)
select * from source_data
