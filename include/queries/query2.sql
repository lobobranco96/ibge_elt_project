-- 2. Evolução da população por região

SELECT 
    r.nome_regiao,
    f.ano,
    SUM(f.populacao) AS populacao_total
FROM fato_populacao f
JOIN dim_estado e ON f.id_estado = e.id_estado
JOIN dim_regiao r ON e.id_regiao = r.id_regiao
GROUP BY r.nome_regiao, f.ano
ORDER BY r.nome_regiao, f.ano;
