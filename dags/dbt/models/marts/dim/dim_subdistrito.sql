{{ config(
    materialized="view"
) }}

with base as (
    select
        "bairro.id" as id_subdistrito,
        "bairro.nome" as nome_subdistrito,
        "distrito.id" as id_distrito,
        "municipio.nome" as nome_municipio
    from {{ ref('stg_subdistritos') }}
)

select
    id_subdistrito,
    nome_subdistrito,
    id_distrito,
    nome_municipio
from base
