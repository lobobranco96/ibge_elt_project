with base as (
    select
        "estado.id" as id_estado,
        "estado.sigla" as sigla_estado,
        "estado.nome" as nome_estado,
        "regiao.id" as id_regiao
    from {{ ref('stg_estados') }}
)

select
    md5(cast(id_estado as text)) as sk_estado,
    *
from base
