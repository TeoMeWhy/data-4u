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

  WHERE t2.dtRelease <= '{date}'

  ORDER BY t1.idGame
)

SELECT idCompany,
       '{date}' AS dtRef,
        COUNT(DISTINCT CASE WHEN descSlug = 'adventure' THEN idGame END) / COUNT(DISTINCT idGame) AS pctGenreAdventure,
        COUNT(DISTINCT CASE WHEN descSlug = 'arcade' THEN idGame END) / COUNT(DISTINCT idGame) AS pctGenreArcade,
        COUNT(DISTINCT CASE WHEN descSlug = 'card-and-board-game' THEN idGame END) / COUNT(DISTINCT idGame) AS pctGenreCardBoard,
        COUNT(DISTINCT CASE WHEN descSlug = 'fighting' THEN idGame END) / COUNT(DISTINCT idGame) AS pctGenreFighting,
        COUNT(DISTINCT CASE WHEN descSlug = 'indie' THEN idGame END) / COUNT(DISTINCT idGame) AS pctGenreIndie,
        COUNT(DISTINCT CASE WHEN descSlug = 'hack-and-slash-beat-em-up' THEN idGame END) / COUNT(DISTINCT idGame) AS pctGenreHackSlashBeatUp,
        COUNT(DISTINCT CASE WHEN descSlug = 'moba' THEN idGame END) / COUNT(DISTINCT idGame) AS pctGenreMoba,
        COUNT(DISTINCT CASE WHEN descSlug = 'music' THEN idGame END) / COUNT(DISTINCT idGame) AS pctGenreMusic,
        COUNT(DISTINCT CASE WHEN descSlug = 'pinball' THEN idGame END) / COUNT(DISTINCT idGame) AS pctGenrePinball,
        COUNT(DISTINCT CASE WHEN descSlug = 'platform' THEN idGame END) / COUNT(DISTINCT idGame) AS pctGenrePlatform,
        COUNT(DISTINCT CASE WHEN descSlug = 'point-and-click' THEN idGame END) / COUNT(DISTINCT idGame) AS pctGenrePointClick,
        COUNT(DISTINCT CASE WHEN descSlug = 'puzzle' THEN idGame END) / COUNT(DISTINCT idGame) AS pctGenrePuzzle,
        COUNT(DISTINCT CASE WHEN descSlug = 'quiz-trivia' THEN idGame END) / COUNT(DISTINCT idGame) AS pctGenreQuizTrivia,
        COUNT(DISTINCT CASE WHEN descSlug = 'racing' THEN idGame END) / COUNT(DISTINCT idGame) AS pctGenreRacing,
        COUNT(DISTINCT CASE WHEN descSlug = 'real-time-strategy-rts' THEN idGame END) / COUNT(DISTINCT idGame) AS pctGenreRTS,
        COUNT(DISTINCT CASE WHEN descSlug = 'role-playing-rpg' THEN idGame END) / COUNT(DISTINCT idGame) AS pctGenreRPG,
        COUNT(DISTINCT CASE WHEN descSlug = 'simulator' THEN idGame END) / COUNT(DISTINCT idGame) AS pctGenreSimulator,
        COUNT(DISTINCT CASE WHEN descSlug = 'shooter' THEN idGame END) / COUNT(DISTINCT idGame) AS pctGenreShooter,
        COUNT(DISTINCT CASE WHEN descSlug = 'sport' THEN idGame END) / COUNT(DISTINCT idGame) AS pctGenrEsport,
        COUNT(DISTINCT CASE WHEN descSlug = 'strategy' THEN idGame END) / COUNT(DISTINCT idGame) AS pctGenrEstrategy,
        COUNT(DISTINCT CASE WHEN descSlug = 'tactical' THEN idGame END) / COUNT(DISTINCT idGame) AS pctGenreTactical,
        COUNT(DISTINCT CASE WHEN descSlug = 'turn-based-strategy-tbs' THEN idGame END) / COUNT(DISTINCT idGame) AS pctGenreTBS,
        COUNT(DISTINCT CASE WHEN descSlug = 'visual-novel' THEN idGame END) / COUNT(DISTINCT idGame) AS pctGenreVisualNovel,
        COUNT(DISTINCT CASE WHEN descSlug IS null THEN idGame END) / COUNT(DISTINCT idGame) AS pctGenreNull

FROM tb_full

GROUP BY 1,2
ORDER BY 1