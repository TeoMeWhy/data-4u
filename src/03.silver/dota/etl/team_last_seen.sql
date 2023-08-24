SELECT idTeam,
       descTeamName,
       descTeamTag,
       urlteamLogo

FROM silver.dota.matches_teams

QUALIFY ROW_NUMBER() OVER (PARTITION BY descTeamName ORDER BY dtMatch) = 1