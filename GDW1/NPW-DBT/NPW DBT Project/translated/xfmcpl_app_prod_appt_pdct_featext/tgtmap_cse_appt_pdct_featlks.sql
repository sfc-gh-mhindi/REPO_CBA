{{ config(materialized='view', tags=['XfmCPL_APP_PROD_Appt_Pdct_FeatExt']) }}

WITH 
_cba__app_csel4_csel4__prd_lookupset_map__cse__appt__pdct__feat AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4__prd_lookupset_map__cse__appt__pdct__feat")  }})
TgtMAP_CSE_APPT_PDCT_FEATLks AS (
	SELECT CAMPAIGN_CAT_ID,
		FEAT_I,
		ACTL_VAL_R
	FROM _cba__app_csel4_csel4__prd_lookupset_map__cse__appt__pdct__feat
)

SELECT * FROM TgtMAP_CSE_APPT_PDCT_FEATLks