WITH tb_all AS (

    SELECT t1.idCompany,
           t1.idGame,
           t1.flDeveloper,
           t1.flPorting,
           t1.flPublisher,
           t1.flSupporting,
           t2.dtRelease,
           t2.dtRelease,
           t2.vlRatingExternal,
           t2.qtdeRatingExternal,
           t2.vlRatingIGDB,
           t2.qtdeRatingIGDB,
           t2.vlRatingTotal,
           t2.qtdeRatingTotal,
           t2.qtdeFollows

    FROM silver.igdb.involved_companies AS t1

    LEFT JOIN silver.igdb.games AS t2
    ON t1.idGame = t2.idGame

    WHERE t2.dtRelease < '{date}'
    AND t2.dtRelease >= date('{date}') - INTERVAL 10 year
)

SELECT idCompany,
       '{date}' AS dtRef,
       count( distinct idGame) AS qtdeGamesInvolved10Years,
       count(distinct CASE WHEN flDeveloper = TRUE THEN idGame END) AS qtdeGamesDeveloper10Years,
       count(distinct CASE WHEN flPorting = TRUE THEN idGame END) AS qtdeGamesPorting10Years,
       count(distinct CASE WHEN flPublisher = TRUE THEN idGame END) AS qtdeGamesPublisher10Years,
       count(distinct CASE WHEN flSupporting = TRUE THEN idGame END) AS qtdeGamesSupporting10Years,
       max(date_diff('{date}', dtRelease)) AS qtdeDaysFirstRelease10Years,
       min(date_diff('{date}', dtRelease)) AS qtdeDaysLastRelease10Years,
       coalesce(avg(vlRatingExternal),0) AS avgRatingExternal10Years ,
       coalesce(avg(qtdeRatingExternal),0) AS avgVotesRatingExternal10Years,
       coalesce(sum(qtdeRatingExternal),0) AS TotalVotesRatingExternal10Years,
       coalesce(avg(vlRatingIGDB),0) AS avgRatingIGDB10Years,
       coalesce(avg(qtdeRatingIGDB),0) AS avgVotesRatingIGDB10Years,
       coalesce(sum(qtdeRatingIGDB),0) AS TotalVotesRatingIGDB10Years,
       coalesce(avg(vlRatingTotal),0) AS avgRatingTotal10Years,
       coalesce(avg(qtdeRatingTotal),0) AS avgVotesRatingTotal10Years,
       coalesce(sum(qtdeRatingTotal),0) AS TotalVotesRatingTotal10Years,
       coalesce(avg(qtdeFollows),0) AS avgFollows10Years,
       coalesce(sum(qtdeFollows),0) AS TotalFollows10Years 

FROM tb_all

GROUP BY idCompany, dtRef