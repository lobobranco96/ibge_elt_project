-- 8. Quantidade de subdistritos para cada municipio no estado RJ

SELECT 
    m.nome_municipio,
    COUNT(s.id_subdistrito) AS total_subdistritos
FROM dim_subdistrito s
JOIN dim_distrito d ON s.id_distrito = d.id_distrito
JOIN dim_municipio m ON d.id_municipio = m.id_municipio
JOIN dim_estado e ON m.id_uf = e.id_estado
WHERE e.nome_estado = 'Rio de Janeiro'
GROUP BY m.nome_municipio
ORDER BY total_subdistritos DESC;
