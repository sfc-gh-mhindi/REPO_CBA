{{ config(materialized='view', tags=['XfmHL_APP_PROD_PURPOSEFrmExt']) }}

WITH 
_cba__app_csel4_csel4dev_lookupset_map__cse__appt__purp__hl__hl__loan__purp__cat__id AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_lookupset_map__cse__appt__purp__hl__hl__loan__purp__cat__id")  }})
SrcMAP_CSE_APPT_PURP_HLLks AS (
	SELECT HL_LOAN_PURPOSE_CAT_ID,
		PURP_TYPE_C
	FROM _cba__app_csel4_csel4dev_lookupset_map__cse__appt__purp__hl__hl__loan__purp__cat__id
)

SELECT * FROM SrcMAP_CSE_APPT_PURP_HLLks