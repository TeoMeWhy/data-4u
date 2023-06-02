SELECT id AS idGame,
       explode(platforms) as idPlatform

FROM bronze.igdb.games
WHERE platforms IS NOT null