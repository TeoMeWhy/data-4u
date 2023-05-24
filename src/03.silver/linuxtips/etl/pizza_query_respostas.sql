DROP TABLE IF EXISTS silver.linuxtips.pizza_query_pedidos;

CREATE TABLE IF NOT EXISTS silver.linuxtips.pizza_query_pedidos
WITH tb_explode_queijo AS (
    SELECT id,
          queijo_1,
          queijo_2,
          explode(split(queijo_1, ',')) as split_queijo_1
    FROM bronze.linuxtips.pizza_query_forms
),

tb_rn_queijo AS (
    SELECT *,
          row_number() OVER (PARTITION BY id ORDER BY split_queijo_1 DESC) AS rnQueijo
    FROM tb_explode_queijo
),

tb_queijos AS (
    SELECT id,
          max(CASE WHEN rnQueijo = 1 THEN split_queijo_1 END) AS descQueijo1,
          max( COALESCE( CASE WHEN rnQueijo = 2 THEN split_queijo_1 END, queijo_2)) AS descQueijo2
    FROM tb_rn_queijo
    GROUP BY id
),

tb_explode_ingredientes AS (
    SELECT id,
          ingrediente_1,
          ingrediente_2,
          ingrediente_3,
          ingrediente_4,
          ingrediente_5,
          explode(split(ingrediente_1, ',')) as split_ingrediente_1
    FROM bronze.linuxtips.pizza_query_forms
),

tb_rn_ingrediente AS (
    SELECT *,
          row_number() OVER (PARTITION BY id ORDER BY split_ingrediente_1 DESC) AS rnIngrediente
    FROM tb_explode_ingredientes
),

tb_ingredientes AS (
    SELECT id,
          max(CASE WHEN rnIngrediente = 1 THEN split_ingrediente_1 END) AS descIngrediente1,
          max( COALESCE( CASE WHEN rnIngrediente = 2 THEN split_ingrediente_1 END, ingrediente_2)) AS descIngrediente2,
          max( COALESCE( CASE WHEN rnIngrediente = 3 THEN split_ingrediente_1 END, ingrediente_3)) AS descIngrediente3,
          max( COALESCE( CASE WHEN rnIngrediente = 4 THEN split_ingrediente_1 END, ingrediente_4)) AS descIngrediente4,
          max( COALESCE( CASE WHEN rnIngrediente = 5 THEN split_ingrediente_1 END, ingrediente_5)) AS descIngrediente5

    FROM tb_rn_ingrediente
    GROUP BY id

),

tb_final AS (

    SELECT int(t1.id) AS idPedido,
          to_timestamp(t1.updated_at, 'dd/MM/yyyy HH:mm:ss') AS dtPedido,
          
          lower(trim(t1.tipo_massa))  AS descTipoMassa,
          lower(trim(t1.borda_recheada))  AS descTipoBorda,
          lower(trim(t2.descQueijo1)) AS descQueijo1,
          lower(trim(t2.descQueijo2)) AS descQueijo2,
          lower(trim(t3.descIngrediente1)) AS descIngrediente1,
          lower(trim(t3.descIngrediente2)) AS descIngrediente2,
          lower(trim(t3.descIngrediente3)) AS descIngrediente3,
          lower(trim(t3.descIngrediente4)) AS descIngrediente4,
          lower(trim(t3.descIngrediente5)) AS descIngrediente5,
          lower(trim(t1.bebida))  AS descBebida,

            CASE WHEN t1.ketchup = 'Sim!' THEN TRUE
                WHEN t1.ketchup = 'Não!' THEN FALSE
            END AS flKetchup,

            t1.estado AS descUF,
            t1.recado_pizzaiolo AS txtRecado

    FROM bronze.linuxtips.pizza_query_forms AS t1

    LEFT JOIN tb_queijos AS t2
    ON t1.id = t2.id

    LEFT JOIN tb_ingredientes AS t3
    ON t1.id = t3.id

    WHERE t1.id IS NOT null

)

SELECT *
from tb_final;