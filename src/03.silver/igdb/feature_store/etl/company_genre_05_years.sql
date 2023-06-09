WITH tb_full AS (
  SELECT t1.idCompany,
        t1.idGame,
        t4.*

  FROM silver.igdb.involved_companies AS t1

  LEFT JOIN silver.igdb.games AS t2
  ON t1.idGame = t2.idGame

  LEFT JOIN silver.igdb.games_genres AS t3
  ON t2.idGame = t3.idGame

  LEFT JOIN silver.igdb.genres as t4
  ON t3.idGenre = t4.idGenre

  WHERE t2.dtRelease < '{date}'
  AND t2.dtRelease >= date('{date}') - INTERVAL 5 year

  ORDER BY t1.idGame
)

SELECT idCompany,
       '{date}' AS dtRef,
        COUNT(DISTINCT CASE WHEN descSlug = 'adventure' THEN idGame END) / COUNT(DISTINCT idGame) AS pctGenreAdventure05Years,
        COUNT(DISTINCT CASE WHEN descSlug = 'arcade' THEN idGame END) / COUNT(DISTINCT idGame) AS pctGenreArcade05Years,
        COUNT(DISTINCT CASE WHEN descSlug = 'card-and-board-game' THEN idGame END) / COUNT(DISTINCT idGame) AS pctGenreCardBoard05Years,
        COUNT(DISTINCT CASE WHEN descSlug = 'fighting' THEN idGame END) / COUNT(DISTINCT idGame) AS pctGenreFighting05Years,
        COUNT(DISTINCT CASE WHEN descSlug = 'indie' THEN idGame END) / COUNT(DISTINCT idGame) AS pctGenreIndie05Years,
        COUNT(DISTINCT CASE WHEN descSlug = 'hack-and-slash-beat-em-up' THEN idGame END) / COUNT(DISTINCT idGame) AS pctGenreHackSlashBeatUp05Years,
        COUNT(DISTINCT CASE WHEN descSlug = 'moba' THEN idGame END) / COUNT(DISTINCT idGame) AS pctGenreMoba05Years,
        COUNT(DISTINCT CASE WHEN descSlug = 'music' THEN idGame END) / COUNT(DISTINCT idGame) AS pctGenreMusic05Years,
        COUNT(DISTINCT CASE WHEN descSlug = 'pinball' THEN idGame END) / COUNT(DISTINCT idGame) AS pctGenrePinball05Years,
        COUNT(DISTINCT CASE WHEN descSlug = 'platform' THEN idGame END) / COUNT(DISTINCT idGame) AS pctGenrePlatform05Years,
        COUNT(DISTINCT CASE WHEN descSlug = 'point-and-click' THEN idGame END) / COUNT(DISTINCT idGame) AS pctGenrePointClick05Years,
        COUNT(DISTINCT CASE WHEN descSlug = 'puzzle' THEN idGame END) / COUNT(DISTINCT idGame) AS pctGenrePuzzle05Years,
        COUNT(DISTINCT CASE WHEN descSlug = 'quiz-trivia' THEN idGame END) / COUNT(DISTINCT idGame) AS pctGenreQuizTrivia05Years,
        COUNT(DISTINCT CASE WHEN descSlug = 'racing' THEN idGame END) / COUNT(DISTINCT idGame) AS pctGenreRacing05Years,
        COUNT(DISTINCT CASE WHEN descSlug = 'real-time-strategy-rts' THEN idGame END) / COUNT(DISTINCT idGame) AS pctGenreRTS05Years,
        COUNT(DISTINCT CASE WHEN descSlug = 'role-playing-rpg' THEN idGame END) / COUNT(DISTINCT idGame) AS pctGenreRPG05Years,
        COUNT(DISTINCT CASE WHEN descSlug = 'simulator' THEN idGame END) / COUNT(DISTINCT idGame) AS pctGenreSimulator05Years,
        COUNT(DISTINCT CASE WHEN descSlug = 'shooter' THEN idGame END) / COUNT(DISTINCT idGame) AS pctGenreShooter05Years,
        COUNT(DISTINCT CASE WHEN descSlug = 'sport' THEN idGame END) / COUNT(DISTINCT idGame) AS pctGenrEsport05Years,
        COUNT(DISTINCT CASE WHEN descSlug = 'strategy' THEN idGame END) / COUNT(DISTINCT idGame) AS pctGenrEstrategy05Years,
        COUNT(DISTINCT CASE WHEN descSlug = 'tactical' THEN idGame END) / COUNT(DISTINCT idGame) AS pctGenreTactical05Years,
        COUNT(DISTINCT CASE WHEN descSlug = 'turn-based-strategy-tbs' THEN idGame END) / COUNT(DISTINCT idGame) AS pctGenreTBS05Years,
        COUNT(DISTINCT CASE WHEN descSlug = 'visual-novel' THEN idGame END) / COUNT(DISTINCT idGame) AS pctGenreVisualNovel05Years,
        COUNT(DISTINCT CASE WHEN descSlug IS null THEN idGame END) / COUNT(DISTINCT idGame) AS pctGenreNull05Years

FROM tb_full

GROUP BY 1,2
ORDER BY 1