SELECT id AS idGame,
       explode(genres) AS idGenre

FROM bronze.igdb.games
WHERE genres IS NOT NULL