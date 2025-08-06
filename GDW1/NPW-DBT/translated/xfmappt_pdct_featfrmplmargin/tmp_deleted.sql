{{ config(materialized='view', tags=['XfmAPPT_PDCT_FEATFrmPlMargin']) }}

WITH 
appt_pdct_feat AS (
	SELECT
	*
	FROM {{ ref("appt_pdct_feat")  }}),
tmp_deleted AS (
	SELECT
	*
	FROM {{ ref("tmp_deleted")  }}),
TMP_DELETED AS (SELECT tmpdlt.TGT_TBL_NAME, tmpdlt.SRC_TBL_NAME, tmpdlt.DLTD_KEY1_VALU, tmpdlt.DLTD_KEY2_VALU, apf.SRCE_SYST_APPT_FEAT_I FROM TMP_DELETED LEFT OUTER JOIN APPT_PDCT_FEAT ON tmpdlt.DLTD_KEY1_VALU = apf.SRCE_SYST_APPT_FEAT_I AND apf.EXPY_D = '9999-12-31' WHERE tmpdlt.TGT_TBL_NAME = 'APPT_PDCT_FEAT' AND tmpdlt.SRC_TBL_NAME = 'PlIntRatePlMarginRejects')


SELECT * FROM TMP_DELETED