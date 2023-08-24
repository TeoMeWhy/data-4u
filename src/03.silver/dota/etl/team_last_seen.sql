SELECT idTeam,
       TRIM(descTeamName) AS descTeamName,
       descTeamTag,
       urlteamLogo

FROM silver.dota.matches_teams

QUALIFY ROW_NUMBER() OVER (PARTITION BY TRIM(descTeamName) ORDER BY dtMatch DESC) = 1