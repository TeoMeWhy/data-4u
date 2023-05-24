
DROP TABLE IF EXISTS silver.linuxtips.pizza_query_produto;

CREATE TABLE IF NOT EXISTS silver.linuxtips.pizza_query_produto
WITH tb_final AS (

  SELECT lower(ingrediente) as descIngrediente,
        float(replace(valor, ',', '.')) as vlPreco

  FROM bronze.linuxtips.pizza_query_produtos

  WHERE lower(ingrediente) <> 'n/a'
  ORDER BY 1
 
 )

 SELECT * FROM tb_final;