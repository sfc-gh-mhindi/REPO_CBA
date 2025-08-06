{{ config(materialized='view', tags=['XfmDelFlagAPPT_PDCT_REL']) }}

WITH 
tmp_deleted AS (
	SELECT
	*
	FROM {{ ref("tmp_deleted")  }}),
appt_pdct_rel AS (
	SELECT
	*
	FROM {{ ref("appt_pdct_rel")  }}),
TMP_DELETED AS (SELECT tmpdlt.TGT_TBL_NAME, tmpdlt.SRC_TBL_NAME, tmpdlt.DLTD_TABL_NAME, tmpdlt.DLTD_KEY1_VALU, apr.RELD_APPT_PDCT_I, apr.APPT_PDCT_I FROM TMP_DELETED INNER JOIN APPT_PDCT_REL ON tmpdlt.DLTD_KEY1_VALU = apr.RELD_APPT_PDCT_I WHERE tmpdlt.TGT_TBL_NAME = 'APPT_PDCT_REL' AND tmpdlt.SRC_TBL_NAME = 'APPT_PDCT_REL' AND apr.EXPY_D = '9999-12-31')


SELECT * FROM TMP_DELETED