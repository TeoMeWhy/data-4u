DROP TABLE IF EXISTS silver.pizza_query.pedido;

CREATE TABLE IF NOT EXISTS silver.pizza_query.pedido

WITH tb_final AS (

    SELECT int(t1.id) AS idPedido,
           to_timestamp(t1.updated_at, 'dd/MM/yyyy HH:mm:ss') AS dtPedido,
          
            CASE WHEN t1.ketchup = 'Sim!' THEN TRUE
                WHEN t1.ketchup = 'Não!' THEN FALSE
            END AS flKetchup,

            t1.estado AS descUF,
            t1.recado_pizzaiolo AS txtRecado

    FROM bronze.linuxtips.pizza_query_forms AS t1
    WHERE t1.id IS NOT null
)

SELECT *
FROM tb_final
ORDER BY idPedido;