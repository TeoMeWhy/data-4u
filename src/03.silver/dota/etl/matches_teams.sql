WITH tb_union AS (

    SELECT match_id,
          start_time,
          radiant_team.*,
          true flIsRadiant
    FROM bronze.dota.matches

    UNION ALL

    SELECT match_id,
          start_time,
          dire_team.*,
          false flIsRadiant
    FROM bronze.dota.matches
),

tb_union_consolidate AS (

    SELECT match_id AS idMatch,
           from_unixtime(start_time) AS dtMatch,
           team_id AS idTeam,
           name AS descTeamName,
           tag AS descTeamTag,
           logo_url AS urlteamLogo,
           flIsRadiant
    FROM tb_union
    WHERE team_id IS NOT NULL
    ORDER BY match_id, flIsRadiant

)

SELECT *
FROM tb_union_consolidate