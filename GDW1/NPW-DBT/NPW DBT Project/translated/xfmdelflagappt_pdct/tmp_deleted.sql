{{ config(materialized='view', tags=['XfmDelFlagAPPT_PDCT']) }}

WITH 
appt_pdct AS (
	SELECT
	*
	FROM {{ ref("appt_pdct")  }}),
tmp_deleted AS (
	SELECT
	*
	FROM {{ ref("tmp_deleted")  }}),
TMP_DELETED AS (SELECT tmpdlt.TGT_TBL_NAME, tmpdlt.SRC_TBL_NAME, tmpdlt.DLTD_TABL_NAME, tmpdlt.DLTD_KEY1_VALU, ap.APPT_PDCT_I, ap.APPT_I FROM TMP_DELETED INNER JOIN APPT_PDCT ON tmpdlt.DLTD_KEY1_VALU = ap.APPT_PDCT_I WHERE tmpdlt.TGT_TBL_NAME = 'APPT_PDCT' AND tmpdlt.SRC_TBL_NAME = 'APPT_PDCT' AND ap.EXPY_D = '9999-12-31')


SELECT * FROM TMP_DELETED