-- 7. Quantidade total de municipios por estado

SELECT 
    e.nome_estado,
    e.sigla_estado,
    COUNT(*) AS total_municipios
FROM dim_municipio m
JOIN dim_estado e ON m.id_uf = e.id_estado
GROUP BY e.nome_estado, e.sigla_estado
ORDER BY total_municipios DESC;
