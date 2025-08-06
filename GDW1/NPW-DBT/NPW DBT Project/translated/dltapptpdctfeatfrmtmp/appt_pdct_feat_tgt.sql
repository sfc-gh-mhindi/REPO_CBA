{{ config(materialized='view', tags=['DltApptPdctFeatFrmTMP']) }}

WITH 
tmp_appt_pdct_feat AS (
	SELECT
	*
	FROM {{ ref("tmp_appt_pdct_feat")  }}),
appt_pdct_feat AS (
	SELECT
	*
	FROM {{ ref("appt_pdct_feat")  }}),
Appt_Pdct_Feat_Tgt AS (SELECT APPT_PDCT_I, FEAT_I, EFFT_D, SRCE_SYST_C, ACTL_VALU_Q FROM APPT_PDCT_FEAT WHERE APPT_PDCT_I IN (SELECT DISTINCT APPT_PDCT_I FROM TMP_APPT_PDCT_FEAT WHERE RUN_STRM = 'CSE_COM_CPO_BUS_NCPR_CLNT') AND EXPY_D = '9999-12-31')


SELECT * FROM Appt_Pdct_Feat_Tgt