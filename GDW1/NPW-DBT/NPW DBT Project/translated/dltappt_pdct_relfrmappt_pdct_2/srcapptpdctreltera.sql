{{ config(materialized='view', tags=['DltAPPT_PDCT_RELFrmAPPT_PDCT_2']) }}

WITH 
appt_pdct_rel AS (
	SELECT
	*
	FROM {{ ref("appt_pdct_rel")  }}),
tmp_gdw_appt_pdct AS (
	SELECT
	*
	FROM {{ ref("tmp_gdw_appt_pdct")  }}),
SrcApptPdctRelTera AS (SELECT b.APPT_PDCT_I, b.RELD_APPT_PDCT_I, b.EFFT_D FROM TMP_GDW_APPT_PDCT, APPT_PDCT_REL WHERE TRIM(a.APPT_PDCT_I) = TRIM(b.APPT_PDCT_I) AND b.EXPY_D = '9999-12-31' AND a.EXPY_FLAG = 'Y')


SELECT * FROM SrcApptPdctRelTera