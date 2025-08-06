{{ config(materialized='view', tags=['XfmDelFlagAPPT_TRNF_DETL']) }}

WITH 
tmp_deleted AS (
	SELECT
	*
	FROM {{ ref("tmp_deleted")  }}),
appt_trnf_detl AS (
	SELECT
	*
	FROM {{ ref("appt_trnf_detl")  }}),
TMP_DELETED AS (SELECT tmpdlt.TGT_TBL_NAME, tmpdlt.SRC_TBL_NAME, tmpdlt.DLTD_TABL_NAME, tmpdlt.DLTD_KEY1_VALU, atd.APPT_TRNF_I, atd.APPT_I FROM TMP_DELETED INNER JOIN APPT_TRNF_DETL ON tmpdlt.DLTD_KEY1_VALU = atd.APPT_TRNF_I WHERE tmpdlt.TGT_TBL_NAME = 'APPT_TRNF_DETL' AND tmpdlt.SRC_TBL_NAME = 'APPT_TRNF_DETL' AND atd.EXPY_D = '9999-12-31')


SELECT * FROM TMP_DELETED