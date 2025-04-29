-- 3. Estados com maior crescimento populacional entre 2014 e 2024

WITH pop_anos AS (
    SELECT 
        id_estado,
        ano,
        populacao
    FROM fato_populacao
    WHERE ano IN (2014, 2024)
)
SELECT 
    e.nome_estado,
    MAX(CASE WHEN ano = 2014 THEN populacao END) AS pop_2014,
    MAX(CASE WHEN ano = 2024 THEN populacao END) AS pop_2024,
    MAX(CASE WHEN ano = 2024 THEN populacao END) - MAX(CASE WHEN ano = 2014 THEN populacao END) AS crescimento
FROM pop_anos p
JOIN dim_estado e ON p.id_estado = e.id_estado
GROUP BY e.nome_estado
ORDER BY crescimento DESC;
