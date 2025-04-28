with base as (
    select
        "bairro.id" as id_bairro,
        "bairro.nome" as nome_bairro,
        "distrito.id" as id_distrito,
        "municipio.nome" as nome_municipio
    from {{ ref('stg_subdistritos') }}
)

select
    md5(cast(id_bairro as text)) as sk_subdistrito,
    *
from base
