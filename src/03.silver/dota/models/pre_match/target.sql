  SELECT idMatch,
         flRadiantWin,
         string(dtMatchDay) AS dtReference,
         idDireTeam AS idTeamDire,
         idRadiantTeam AS idTeamRadiant

  FROM silver.dota.matches

  WHERE dtMatchDay >= '2018-01-01'
  AND dtMatchDay < '2023-08-24'
  AND idDireTeam IS NOT NULL
  AND idRadiantTeam IS NOT NULL