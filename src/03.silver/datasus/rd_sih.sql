-- Databricks notebook source
DROP TABLE IF EXISTS silver.datasus.rd_sih;

CREATE TABLE IF NOT EXISTS silver.datasus.rd_sih AS

WITH tb_uf AS (
  select distinct codUF, descUF
  from silver.ibge.municipios_brasileiros
),

tb_cid AS (
  select *
  from silver.datasus.cid
  where codCID not like '%-%'
  and codCID10 not like '%.%'
  qualify row_number() over (partition by codCID10dst order by codCID desc) = 1
)

SELECT 
    -- t1.UF_ZI,
    t32.descUF,
    t1.ANO_CMPT,
    t1.MES_CMPT,
    t4.descEspecialidade,
    t1.CGC_HOSP,
    t1.N_AIH,
    t6.descIdentificacaoAIH,
    t1.CEP,
    t1.MUNIC_RES,
    t3.descSexo,
    t1.UTI_MES_IN,
    t1.UTI_MES_AN,
    t1.UTI_MES_AL,
    t1.UTI_MES_TO,
    t8.descMarcaUTI,
    t1.UTI_INT_IN,
    t1.UTI_INT_AN,
    t1.UTI_INT_AL,
    t1.UTI_INT_TO,
    t1.DIAR_ACOM,
    t1.QT_DIARIAS,
    t1.PROC_SOLIC,
    t1.PROC_REA,
    FLOAT(REPLACE(t1.VAL_SH, ',', '.')) AS vlSH,
    FLOAT(REPLACE(t1.VAL_SP, ',', '.')) AS vlSP,
    FLOAT(REPLACE(t1.VAL_SADT, ',', '.')) AS vlSADT,
    FLOAT(REPLACE(t1.VAL_RN, ',', '.')) AS vlRN,
    FLOAT(REPLACE(t1.VAL_ACOMP, ',', '.')) AS vlACOMP,
    FLOAT(REPLACE(t1.VAL_ORTP, ',', '.')) AS vlORTP,
    FLOAT(REPLACE(t1.VAL_SANGUE, ',', '.')) AS vlSANGUE,
    FLOAT(REPLACE(t1.VAL_SADTSR, ',', '.')) AS vlSADTSR,
    FLOAT(REPLACE(t1.VAL_TRANSP, ',', '.')) AS vlTRANSP,
    FLOAT(REPLACE(t1.VAL_OBSANG, ',', '.')) AS vlOBSANG,
    FLOAT(REPLACE(t1.VAL_PED1AC, ',', '.')) AS vlPED1AC,
    FLOAT(REPLACE(t1.VAL_TOT, ',', '.')) AS vlTOT,
    FLOAT(REPLACE(t1.VAL_UTI, ',', '.')) AS vlUTI,
    FLOAT(REPLACE(t1.US_TOT, ',', '.')) AS vlUSTOT,
    TO_DATE(t1.DT_INTER, 'yyyymmdd') AS DtInternacao,
    TO_DATE(t1.DT_SAIDA, 'yyyymmdd') AS DtSaida,
    t1.DIAG_PRINC,
    t33.descCID AS descDiagnosticoCIDPrinc,
    t1.DIAG_SECUN,
    t34.descCID AS descDiagnosticoCIDSec,
    t17.`descTipoCobrança` AS descTipoCobranca,
    t10.descNaturezaHospitalSUS,
    t11.descNaturezaJuridica,
    t1.GESTAO,
    t14.descRubrica,
    t1.IND_VDRL,
    t1.MUNIC_MOV,
    t1.COD_IDADE,
    t1.IDADE,
    t1.DIAS_PERM,
    t1.MORTE,
    t9.descNacionalidade,
    t2.descCaraterInternacao,
    t1.TOT_PT_SP,
    t1.CPF_AUT,
    t1.HOMONIMO,
    int(t1.NUM_FILHOS) AS nrFilhos,
    t5.descGrauInstrucao,
    t1.CID_NOTIF,
    t18.descTipoContraceptivo AS descTipoContraceptivo1,
    t19.descTipoContraceptivo AS descTipoContraceptivo2,
    t1.GESTRISCO,
    t1.INSC_PN,
    t15.descSeqAIH,
    t1.CBOR,
    t1.CNAER,
    t22.descTipoVincPrev,
    t1.GESTOR_COD,
    t21.descTipoGestao,
    t1.GESTOR_CPF,
    t1.GESTOR_DT,
    t1.CNES,
    t1.CNPJ_MANT,
    t1.INFEHOSP,
    t1.CID_ASSO,
    t1.CID_MORTE,
    t1.COMPLEX,
    t20.descTipoFinanc,
    t16.descSubTipoFinanciamento,
    t13.descRegraContrato,
    t12.descRacaCor,
    t1.ETNIA,
    t1.SEQUENCIA,
    t1.REMESSA,
    t1.AUD_JUST,
    t1.SIS_JUST,
    FLOAT(REPLACE(t1.VAL_SH_FED, ',','.')) AS vlSHFED,
    FLOAT(REPLACE(t1.VAL_SP_FED, ',','.')) AS vlSPFED,
    FLOAT(REPLACE(t1.VAL_SH_GES, ',','.')) AS vlSHGES,
    FLOAT(REPLACE(t1.VAL_SP_GES, ',','.')) AS vlSPGES,
    FLOAT(REPLACE(t1.VAL_UCI, ',','.')) AS vlUCI,
    t7.descMarcaUCI,
    t23.descTpDiagSecundario AS descTpDiagSecundario1,
    t24.descTpDiagSecundario AS descTpDiagSecundario2,
    t25.descTpDiagSecundario AS descTpDiagSecundario3,
    t26.descTpDiagSecundario AS descTpDiagSecundario4,
    t27.descTpDiagSecundario AS descTpDiagSecundario5,
    t28.descTpDiagSecundario AS descTpDiagSecundario6,
    t29.descTpDiagSecundario AS descTpDiagSecundario7,
    t30.descTpDiagSecundario AS descTpDiagSecundario8,
    t31.descTpDiagSecundario AS descTpDiagSecundario9,
    t1.TPDISEC1,
    t1.TPDISEC2,
    t1.TPDISEC3,
    t1.TPDISEC4,
    t1.TPDISEC5,
    t1.TPDISEC6,
    t1.TPDISEC7,
    t1.TPDISEC8,
    t1.TPDISEC9

FROM bronze.datasus.rd_sih AS t1

LEFT JOIN bronze.datasus.carater_internacao AS t2
ON t1.CAR_INT = t2.codCaraterInterncao

LEFT JOIN bronze.datasus.sexo AS t3
ON t1.SEXO = t3.codSexo

LEFT JOIN bronze.datasus.especialidade AS t4
ON t1.ESPEC = t4.codEspecialidade

LEFT JOIN bronze.datasus.grau_instrucao as t5
ON t1.INSTRU = t5.codGrauInstrucao

LEFT JOIN bronze.datasus.ident_aih AS t6
ON t1.IDENT = t6.codIdentificacaoAIH

LEFT JOIN bronze.datasus.marca_uci AS t7
ON t1.MARCA_UCI = t7.codMarcaUCI

LEFT JOIN bronze.datasus.marca_uti AS t8
ON t1.MARCA_UTI = t8.codMarcaUTI

LEFT JOIN bronze.datasus.nacionalidade AS t9
ON t1.NACIONAL = t9.codNacionalidade

LEFT JOIN bronze.datasus.natureza_hospsus AS t10
ON t1.NATUREZA = t10.codNaturezaHospitalSUS

LEFT JOIN bronze.datasus.natureza_juridic AS t11
ON t1.NAT_JUR = t11.codNaturezaJuridica

LEFT JOIN bronze.datasus.raca_cor As t12
ON t1.RACA_COR = t12.codRacaCor

LEFT JOIN bronze.datasus.regra_contrat AS t13
ON t1.REGCT = t13.codRegraContrato

LEFT JOIN bronze.datasus.rubrica AS t14
ON t1.RUBRICA = t14.codRubrica

LEFT JOIN bronze.datasus.seqaih_5 AS t15
ON t1.SEQ_AIH5 = t15.codSeqAIH

LEFT JOIN bronze.datasusOutras complicações da gravidez e do parto
.subtipo_financ AS t16
ON t1.FAEC_TP = t16.codSubTipoFinanciamento

LEFT JOIN bronze.datasus.tipo_cobranca AS t17
ON t1.COBRANCA = t17.`codTipoCobrança`

LEFT JOIN bronze.datasus.tipo_contraceptivo AS t18
ON t1.CONTRACEP1 = t18.codTipoContraceptivo

LEFT JOIN bronze.datasus.tipo_contraceptivo AS t19
ON t1.CONTRACEP2 = t19.codTipoContraceptivo

LEFT JOIN bronze.datasus.tipo_financ AS t20
ON t1.FINANC = t20.codTipoFinanc

LEFT JOIN bronze.datasus.tipo_gestao AS t21
ON t1.GESTOR_TP = t21.codTipoGestao

LEFT JOIN bronze.datasus.tipo_vincprev AS t22
ON t1.VINCPREV = t22.codTipoVincPrev

LEFT JOIN bronze.datasus.tp_diagsecundario as t23
ON t1.DIAGSEC1 = t23.codTpDiagSecundario

LEFT JOIN bronze.datasus.tp_diagsecundario as t24
ON t1.DIAGSEC2 = t24.codTpDiagSecundario

LEFT JOIN bronze.datasus.tp_diagsecundario as t25
ON t1.DIAGSEC3 = t25.codTpDiagSecundario

LEFT JOIN bronze.datasus.tp_diagsecundario as t26
ON t1.DIAGSEC4 = t26.codTpDiagSecundario

LEFT JOIN bronze.datasus.tp_diagsecundario as t27
ON t1.DIAGSEC5 = t27.codTpDiagSecundario

LEFT JOIN bronze.datasus.tp_diagsecundario as t28
ON t1.DIAGSEC6 = t28.codTpDiagSecundario

LEFT JOIN bronze.datasus.tp_diagsecundario as t29
ON t1.DIAGSEC7 = t29.codTpDiagSecundario

LEFT JOIN bronze.datasus.tp_diagsecundario as t30
ON t1.DIAGSEC8 = t30.codTpDiagSecundario

LEFT JOIN bronze.datasus.tp_diagsecundario as t31
ON t1.DIAGSEC9 = t31.codTpDiagSecundario

LEFT JOIN tb_uf AS t32
ON substring(t1.UF_ZI,0,2) = t32.codUF

LEFT JOIN tb_cid AS t33
ON substring(t1.DIAG_PRINC,0,3) = t33.codCID10dst

LEFT JOIN tb_cid AS t34
ON substring(t1.DIAG_SECUN,0,3) = t34.codCID10dst
;
