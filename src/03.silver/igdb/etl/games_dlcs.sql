SELECT id AS idGame,
       explode(dlcs) AS idGameDLC

FROM bronze.igdb.games

WHERE dlcs IS NOT null