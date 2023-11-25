WITH tb_explode AS (

  SELECT explode(players) as players
  FROM {table}

),

SELECT players.*
FROM tb_explode
QUALIFY row_number() OVER (PARTITION BY players.account_id, players.match_id ORDER BY players.start_time DESC) = 1