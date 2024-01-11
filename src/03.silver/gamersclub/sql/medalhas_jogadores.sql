SELECT int(id) AS id, 
       int(idPlayer) AS idJogador,
       int(idMedal) AS idMedalha,
       date(dtCreatedAt) AS dtCriacao,
       date(dtExpiration) AS dtExpiracao,
       date(dtRemove) AS dtRemocao,
       int(flActive) AS flAtiva

FROM bronze.gamersclub.tb_players_medalha