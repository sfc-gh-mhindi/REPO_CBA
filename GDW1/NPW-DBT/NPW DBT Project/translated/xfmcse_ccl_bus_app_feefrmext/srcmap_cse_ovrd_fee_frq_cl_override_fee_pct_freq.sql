{{ config(materialized='view', tags=['XfmCSE_CCL_BUS_APP_FEEFrmExt']) }}

WITH 
_cba__app_csel4_csel4dev_lookupset_map__cse__ovrd__fee__frq__cl__override__fee__pct__freq AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_lookupset_map__cse__ovrd__fee__frq__cl__override__fee__pct__freq")  }})
SrcMAP_CSE_OVRD_FEE_FRQ_CL_OVERRIDE_FEE_PCT_FREQ AS (
	SELECT OVRD_FEE_PCT_FREQ,
		FREQ_IN_MTHS
	FROM _cba__app_csel4_csel4dev_lookupset_map__cse__ovrd__fee__frq__cl__override__fee__pct__freq
)

SELECT * FROM SrcMAP_CSE_OVRD_FEE_FRQ_CL_OVERRIDE_FEE_PCT_FREQ