{{ config(materialized='view', tags=['DltAPPT_PDCT_ACCT_FrmTMP_APPT_PDCT_ACCT']) }}

WITH 
appt_pdct_acct AS (
	SELECT
	*
	FROM {{ ref("appt_pdct_acct")  }}),
tmp_appt_pdct_acct AS (
	SELECT
	*
	FROM {{ ref("tmp_appt_pdct_acct")  }}),
SrcTmpApptPdctAcctTera AS (SELECT a.APPT_PDCT_I AS NEW_APPT_PDCT_I, a.ACCT_I AS NEW_ACCT_I, a.REL_TYPE_C AS NEW_REL_TYPE_C, b.APPT_PDCT_I AS OLD_APPT_PDCT_I, b.ACCT_I AS OLD_ACCT_I, b.REL_TYPE_C AS OLD_REL_TYPE_C, b.EFFT_D AS OLD_EFFT_D FROM TMP_APPT_PDCT_ACCT LEFT OUTER JOIN APPT_PDCT_ACCT ON TRIM(a.APPT_PDCT_I) = TRIM(b.APPT_PDCT_I) AND b.EXPY_D = '9999-12-31' AND b.REL_TYPE_C = 'HLMC' WHERE a.RUN_STRM = '{{ var("RUN_STREAM") }}')


SELECT * FROM SrcTmpApptPdctAcctTera