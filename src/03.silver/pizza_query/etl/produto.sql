
DROP TABLE IF EXISTS silver.pizza_query.produto;

CREATE TABLE IF NOT EXISTS silver.pizza_query.produto
WITH tb_final AS (

  SELECT lower(ingrediente) as descItem,
        float(replace(valor, ',', '.')) as vlPreco

  FROM bronze.linuxtips.pizza_query_produtos

  WHERE lower(ingrediente) <> 'n/a'
  ORDER BY 1
 
 )

 SELECT * FROM tb_final;