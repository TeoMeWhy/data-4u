SELECT 
       int(idPlayer) AS idJogador,
       case when int(flFacebook) = 1 then TRUE else FALSE end as flFacebook,
       case when int(flTwitter) = 1 then TRUE else FALSE end as flTwitter,
       case when int(flTwitch) = 1 then TRUE else FALSE end as flTwitch,
       descCountry AS descPais,
       date(dtBirth) AS dtNascimento,
       timestamp(dtRegistration) AS dtCadastro

FROM bronze.gamersclub.tb_players