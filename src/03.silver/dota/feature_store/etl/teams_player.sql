WITH tb_team_last_match AS (

  SELECT *
  FROM silver.dota.matches_teams AS t1
  WHERE dtMatch < '{date}'
  QUALIFY row_number() OVER (PARTITION BY t1.idTeam ORDER BY t1.dtMatch DESC) = 1
  ORDER BY t1.dtMatch DESC

),

tb_team_players AS (

    SELECT
            t1.idTeam,
            t1.descTeamName,
            t1.descTeamTag,
            t2.idPlayer

    FROM tb_team_last_match AS t1

    LEFT JOIN silver.dota.matches_players AS t2
    ON t1.idMatch = t2.idMatch
    AND t1.flIsradiant = t2.flIsRadiant

    ORDER BY t1.dtMatch, t1.idTeam
)

SELECT '{date}' AS dtReference,
       *
FROM tb_team_players