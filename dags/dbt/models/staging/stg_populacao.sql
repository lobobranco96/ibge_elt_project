with source_data as (
    select
        "localidade.id",
        "localidade.nivel.nome",
        "localidade.nome",
        "serie.2014",
        "serie.2015",
        "serie.2016",
        "serie.2017",
        "serie.2018",
        "serie.2019",
        "serie.2020",
        "serie.2021",
        "serie.2024"
    from {{ source('raw', 'populacao') }}
)
select * from source_data
