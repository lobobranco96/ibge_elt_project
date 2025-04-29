/* {{ config(
    file_format="parquet",
    materialized="table"
) }}
*/
with source_data as (
    select
        "bairro.id",
        "bairro.nome",
        "distrito.id",
        "municipio.nome"
    from {{ source('raw', 'subdistritos') }}
)
select * from source_data
