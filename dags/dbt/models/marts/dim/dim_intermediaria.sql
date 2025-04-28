with base as (
    select
        "intermediaria.id" as id_intermediaria,
        "intermediaria.nome" as nome_intermediaria,
        "uf.id" as id_uf,
        "uf.sigla" as sigla_uf
    from {{ ref('stg_intermediarias') }}
)

select
    *
from base
