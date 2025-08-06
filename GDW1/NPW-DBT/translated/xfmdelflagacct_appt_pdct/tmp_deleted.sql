{{ config(materialized='view', tags=['XfmDelFlagACCT_APPT_PDCT']) }}

WITH 
acct_appt_pdct AS (
	SELECT
	*
	FROM {{ ref("acct_appt_pdct")  }}),
tmp_deleted AS (
	SELECT
	*
	FROM {{ ref("tmp_deleted")  }}),
TMP_DELETED AS (SELECT tmpdlt.TGT_TBL_NAME, tmpdlt.SRC_TBL_NAME, tmpdlt.DLTD_TABL_NAME, tmpdlt.DLTD_KEY1_VALU, aap.APPT_PDCT_I, aap.ACCT_I FROM TMP_DELETED INNER JOIN ACCT_APPT_PDCT ON tmpdlt.DLTD_KEY1_VALU = aap.APPT_PDCT_I WHERE tmpdlt.TGT_TBL_NAME = 'ACCT_APPT_PDCT' AND tmpdlt.SRC_TBL_NAME = 'ACCT_APPT_PDCT' AND aap.EXPY_D = '9999-12-31')


SELECT * FROM TMP_DELETED