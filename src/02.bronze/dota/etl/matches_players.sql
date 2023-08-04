WITH tb_explode AS (

  SELECT explode(players) as players
  FROM {table}

)

SELECT players.*
FROM tb_explode
QUALIFY row_number() OVER (PARTITION BY account_id, match_id ORDER BY start_time DESC) = 1