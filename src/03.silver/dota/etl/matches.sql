WITH tb_match AS (

  SELECT
      match_id AS idMatch,
      radiant_win AS flRadiantWin,
      from_unixtime(start_time) AS dtMatch,
      date(from_unixtime(start_time)) AS dtMatchDay,
      duration AS nrDurationSeconds,
      dire_team_id AS idDireTeam,
      dire_team.name AS descDireTeamName,
      radiant_team_id AS idRadiantTeam,
      radiant_team.name AS descRadiantTeamName,
      dire_score AS nrDireScore,
      radiant_score AS nrRadiantScore,
      leagueid AS idLeague,
      region AS idRegion,
      replay_url AS urlReplay

  FROM bronze.dota.matches
)

SELECT *
FROM tb_match