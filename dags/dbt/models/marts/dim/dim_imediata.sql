with base as (
    select
        "imediata.id" as id_imediata,
        "imediata.nome" as nome_imediata,
        "regiao-intermediaria.id" as id_intermediaria,
        "UF.sigla" as sigla_uf,
        "UF.nome" as nome_uf
    from {{ ref('stg_imediatas') }}
)

select
    md5(cast(id_imediata as text)) as sk_imediata,
    *
from base
