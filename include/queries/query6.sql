-- 6. Municipios do Rio de janeiro(UF)

SELECT 
    m.id_municipio,
    m.nome_municipio
FROM dim_municipio m
JOIN dim_estado e ON m.id_uf = e.id_estado
WHERE e.nome_estado = 'Rio de Janeiro'
ORDER BY m.nome_municipio;
