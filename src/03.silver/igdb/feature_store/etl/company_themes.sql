WITH tb_full AS(

    SELECT t1.idCompany,
          t1.idGame,
          t2.dtRelease,
          t4.descName

    FROM silver.igdb.involved_companies AS t1

    LEFT JOIN silver.igdb.games AS t2
    ON t1.idGame = t2.idGame

    LEFT JOIN silver.igdb.games_themes AS t3
    ON t1.idGame = t3.idGame

    LEFT JOIN silver.igdb.themes AS t4
    ON t3.idTheme = t4.idTheme

    WHERE t2.dtRelease < '{date}'

)

SELECT 

    idCompany,
    '{date}' As dtRef,
    count(DISTINCT CASE WHEN descName = null THEN idGame END) / COUNT(DISTINCT idGame) AS pctNull,
    count(DISTINCT CASE WHEN descName = '4X (explore, expand, exploit, and exterminate)' THEN idGame END) / COUNT(DISTINCT idGame) AS pctEx4,
    count(DISTINCT CASE WHEN descName = 'Action' THEN idGame END) / COUNT(DISTINCT idGame) AS pctAction,
    count(DISTINCT CASE WHEN descName = 'Business' THEN idGame END) / COUNT(DISTINCT idGame) AS pctBusiness,
    count(DISTINCT CASE WHEN descName = 'Comedy' THEN idGame END) / COUNT(DISTINCT idGame) AS pctComedy,
    count(DISTINCT CASE WHEN descName = 'Drama' THEN idGame END) / COUNT(DISTINCT idGame) AS pctDrama,
    count(DISTINCT CASE WHEN descName = 'Educational' THEN idGame END) / COUNT(DISTINCT idGame) AS pctEducational,
    count(DISTINCT CASE WHEN descName = 'Erotic' THEN idGame END) / COUNT(DISTINCT idGame) AS pctErotic,
    count(DISTINCT CASE WHEN descName = 'Fantasy' THEN idGame END) / COUNT(DISTINCT idGame) AS pctFantasy,
    count(DISTINCT CASE WHEN descName = 'Historical' THEN idGame END) / COUNT(DISTINCT idGame) AS pctHistorical,
    count(DISTINCT CASE WHEN descName = 'Horror' THEN idGame END) / COUNT(DISTINCT idGame) AS pctHorror,
    count(DISTINCT CASE WHEN descName = 'Kids' THEN idGame END) / COUNT(DISTINCT idGame) AS pctKids,
    count(DISTINCT CASE WHEN descName = 'Mystery' THEN idGame END) / COUNT(DISTINCT idGame) AS pctMystery,
    count(DISTINCT CASE WHEN descName = 'Non-fiction' THEN idGame END) / COUNT(DISTINCT idGame) AS pctNonfiction,
    count(DISTINCT CASE WHEN descName = 'Open world' THEN idGame END) / COUNT(DISTINCT idGame) AS pctOpenworld,
    count(DISTINCT CASE WHEN descName = 'Party' THEN idGame END) / COUNT(DISTINCT idGame) AS pctParty,
    count(DISTINCT CASE WHEN descName = 'Romance' THEN idGame END) / COUNT(DISTINCT idGame) AS pctRomance,
    count(DISTINCT CASE WHEN descName = 'Sandbox' THEN idGame END) / COUNT(DISTINCT idGame) AS pctSandbox,
    count(DISTINCT CASE WHEN descName = 'Science fiction' THEN idGame END) / COUNT(DISTINCT idGame) AS pctScienceFiction,
    count(DISTINCT CASE WHEN descName = 'Stealth' THEN idGame END) / COUNT(DISTINCT idGame) AS pctStealth,
    count(DISTINCT CASE WHEN descName = 'Survival' THEN idGame END) / COUNT(DISTINCT idGame) AS pctSurvival,
    count(DISTINCT CASE WHEN descName = 'Thriller' THEN idGame END) / COUNT(DISTINCT idGame) AS pctThriller,
    count(DISTINCT CASE WHEN descName = 'Warfare' THEN idGame END) / COUNT(DISTINCT idGame) AS pctWarfare

 FROM tb_full

 GROUP BY 1, 2