schemas = {
    "regioes": {
        "regiao.id": int,
        "regiao.sigla": str,
        "regiao.nome": str,
    },
    "estados": {
        "estado.id": int,
        "estado.sigla": str,
        "estado.nome": str,
        "regiao.id": int,
    },
    "intermediarias": {
        "intermediaria.id": int,
        "intermediaria.nome": str,
        "uf.id": int,
        "uf.sigla": str,
    },
    "imediatas": {
        "imediata.id": int,
        "imediata.nome": str,
        "regiao-intermediaria.id": int,
        "UF.sigla": str,
        "UF.nome": str},
    "municipios": {
        "municipio.id": int,
        "municipio.nome": str,
        "microrregiao.id": int,
        "microrregiao.nome": str,
        "mesorregiao.id": int,
        "mesorregiao.nome": str,
        "mesorregiao.UF.id": int,
        "mesorregiao.UF.sigla": str,
        "mesorregiao.UF.nome": str
    },
    "distritos": {
        "distrito.id": int,
        "distrito.nome": str,
        "municipio.id": int,
        "regiao-imediata.regiao-intermediaria.UF.nome": str
    },
    "subdistritos": {
        "bairro.id": int,
        "bairro.nome": str,
        "distrito.id": int,
        "municipio.nome": str
    },
    "populacao": {
        "localidade.id": str,
        "localidade.nivel.nome": str,
        "localidade.nome": str,
        "serie.2014": str,
        "serie.2015": str,
        "serie.2016": str,
        "serie.2017": str,
        "serie.2018": str,
        "serie.2019": str,
        "serie.2020": str,
        "serie.2021": str,
        "serie.2024": str
    }
}
