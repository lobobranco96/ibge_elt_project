-- 1 - População total no ano 2024

SELECT 
    e.nome_estado,
    f.ano,
    SUM(f.populacao) AS populacao_total
FROM fato_populacao f
JOIN dim_estado e ON f.id_estado = e.id_estado
where f.ano = 2024
GROUP BY e.nome_estado, f.ano
ORDER BY populacao_total DESC;
