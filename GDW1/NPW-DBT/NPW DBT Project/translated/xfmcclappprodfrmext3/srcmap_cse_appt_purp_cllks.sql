{{ config(materialized='view', tags=['XfmCclAppProdFrmExt3']) }}

WITH 
_cba__app_csel4_dev_lookupset_map__cse__appt__purp__cl__ccl__loan__purpose__cat__id AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_lookupset_map__cse__appt__purp__cl__ccl__loan__purpose__cat__id")  }})
SrcMAP_CSE_APPT_PURP_CLLks AS (
	SELECT CCL_LOAN_PURPOSE_CAT_ID,
		PURP_TYPE_C
	FROM _cba__app_csel4_dev_lookupset_map__cse__appt__purp__cl__ccl__loan__purpose__cat__id
)

SELECT * FROM SrcMAP_CSE_APPT_PURP_CLLks