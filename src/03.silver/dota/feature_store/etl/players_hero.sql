WITH tb_matches_players AS (
    SELECT 
          *
    FROM silver.dota.matches_players
    WHERE dtMatch >= '{date}' - INTERVAL {window} DAY
    AND dtMatch < '{date}'
),

tb_players_hero AS (
    SELECT
      '{date}' AS dtReference,
      idPlayer,
      idHero,
      count(distinct idMatch) AS nrFreqPlayerHero,
      avg(flWin) AS pctWinRatePlayerHero,
      avg(nrGoldMinute) AS nrGoldMinutePlayerHero,
      avg(nrXpMinute) AS nrXpMinutePlayerHero
    FROM tb_matches_players
    GROUP BY dtReference, idPlayer, idHero
),

tb_hero AS (
  SELECT idHero,
          avg(flWin) AS pctWinRateHero,
          avg(nrGoldMinute) AS nrGoldMinuteHero,
          avg(nrXpMinute) AS nrXpMinuteHero
  FROM tb_matches_players
  GROUP BY idHero
)

SELECT t1.*,
        t1.pctWinRatePlayerHero / t2.pctWinRateHero AS pctRateWinRatePlayerHero,
        t1.nrGoldMinutePlayerHero / t2.nrGoldMinuteHero AS pctRateGoldMinutePlayerHero,
        t1.nrXpMinutePlayerHero / t2.nrXpMinuteHero AS pctRateXpMinutePlayerHero

FROM tb_players_hero AS t1

LEFT JOIN tb_hero AS t2
ON t1.idHero = t2.idHero