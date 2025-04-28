with base as (
    select
        cast("localidade.id" as text) as id_localidade,
        "localidade.nome" as nome_localidade,
        "localidade.nivel.nome" as nivel_localidade,
        cast("serie.2014" as int) as populacao_2014,
        cast("serie.2015" as int) as populacao_2015,
        cast("serie.2016" as int) as populacao_2016,
        cast("serie.2017" as int) as populacao_2017,
        cast("serie.2018" as int) as populacao_2018,
        cast("serie.2019" as int) as populacao_2019,
        cast("serie.2020" as int) as populacao_2020,
        cast("serie.2021" as int) as populacao_2021,
        cast("serie.2024" as int) as populacao_2024
    from {{ ref('stg_populacao') }}
)

, unpivot as (
    select id_localidade, nome_localidade, nivel_localidade, 2014 as ano, populacao_2014 as populacao from base
    union all
    select id_localidade, nome_localidade, nivel_localidade, 2015, populacao_2015 from base
    union all
    select id_localidade, nome_localidade, nivel_localidade, 2016, populacao_2016 from base
    union all
    select id_localidade, nome_localidade, nivel_localidade, 2017, populacao_2017 from base
    union all
    select id_localidade, nome_localidade, nivel_localidade, 2018, populacao_2018 from base
    union all
    select id_localidade, nome_localidade, nivel_localidade, 2019, populacao_2019 from base
    union all
    select id_localidade, nome_localidade, nivel_localidade, 2020, populacao_2020 from base
    union all
    select id_localidade, nome_localidade, nivel_localidade, 2021, populacao_2021 from base
    union all
    select id_localidade, nome_localidade, nivel_localidade, 2024, populacao_2024 from base
)

select
    md5(cast(id_localidade || '-' || cast(ano as text) as text)) as sk_fato_populacao,
    *
from unpivot
where populacao is not null
