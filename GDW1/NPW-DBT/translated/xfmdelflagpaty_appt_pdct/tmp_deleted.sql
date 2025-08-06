{{ config(materialized='view', tags=['XfmDelFlagPATY_APPT_PDCT']) }}

WITH 
tmp_deleted AS (
	SELECT
	*
	FROM {{ ref("tmp_deleted")  }}),
paty_appt_pdct AS (
	SELECT
	*
	FROM {{ ref("paty_appt_pdct")  }}),
TMP_DELETED AS (SELECT tmpdlt.TGT_TBL_NAME, tmpdlt.SRC_TBL_NAME, tmpdlt.DLTD_TABL_NAME, tmpdlt.DLTD_KEY1_VALU, tmpdlt.DLTD_KEY2_VALU, tmpdlt.DLTD_KEY3_VALU, tmpdlt.DLTD_KEY4_VALU, tmpdlt.DLTD_KEY5_VALU, pap.APPT_PDCT_I, pap.PATY_I FROM TMP_DELETED INNER JOIN PATY_APPT_PDCT ON tmpdlt.DLTD_KEY1_VALU = pap.APPT_PDCT_I AND tmpdlt.DLTD_KEY5_VALU = pap.PATY_ROLE_C WHERE tmpdlt.TGT_TBL_NAME = 'PATY_APPT_PDCT' AND tmpdlt.SRC_TBL_NAME = 'PATY_APPT_PDCT' AND pap.EXPY_D = '9999-12-31')


SELECT * FROM TMP_DELETED