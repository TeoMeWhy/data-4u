with union_tables as (

  select * from bronze_tse.bem_candidato_2018_brasil

  union all

  select * from bronze_tse.bem_candidato_2020_brasil

  union all 

  select * from bronze_tse.bem_candidato_2022_brasil
)

select
      to_date(DT_GERACAO, 'd/M/y') as DT_GERACAO,
      HH_GERACAO,
      ANO_ELEICAO,
      CD_TIPO_ELEICAO,
      NM_TIPO_ELEICAO,
      CD_ELEICAO,
      DS_ELEICAO,
      DT_ELEICAO,
      SG_UF,
      SG_UE,
      NM_UE,
      SQ_CANDIDATO,
      NR_ORDEM_CANDIDATO,
      CD_TIPO_BEM_CANDIDATO,
      DS_TIPO_BEM_CANDIDATO,
      DS_BEM_CANDIDATO,
      CAST( REPLACE(VR_BEM_CANDIDATO, ',', '.') AS FLOAT) AS VR_BEM_CANDIDATO,
      to_date(DT_ULTIMA_ATUALIZACAO, 'd/M/y') as DT_ULTIMA_ATUALIZACAO,
      HH_ULTIMA_ATUALIZACAO

from union_tables