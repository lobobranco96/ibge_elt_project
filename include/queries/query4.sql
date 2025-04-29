 -- 4. População total por cada ano

SELECT 
    f.ano,
    SUM(f.populacao) AS populacao_total
FROM fato_populacao f
GROUP BY f.ano
ORDER BY f.ano;

