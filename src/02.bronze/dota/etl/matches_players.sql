WITH tb_explode AS (

  SELECT players.*
  FROM {table}
  LATERAL VIEW explode(players) FROM {table} AS players

)

SELECT *
FROM tb_explode
QUALIFY row_number() OVER (PARTITION BY account_id, match_id ORDER BY start_time DESC) = 1