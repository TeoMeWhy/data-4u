SELECT id AS idGame,
       explode(expansions) AS idGameExpansion

FROM bronze.igdb.games

WHERE expansions IS NOT null