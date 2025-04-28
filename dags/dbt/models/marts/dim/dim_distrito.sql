with base as (
    select
        "distrito.id" as id_distrito,
        "distrito.nome" as nome_distrito,
        "municipio.id" as id_municipio,
        "regiao-imediata.regiao-intermediaria.UF.nome" as nome_uf
    from {{ ref('stg_distritos') }}
)

select
    *
from base
