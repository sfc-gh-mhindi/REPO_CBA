{{ config(materialized='view', tags=['XfmAppCclAppChlAppFrmExt']) }}

WITH 
_cba__app_csel4_dev_lookupset_map__cse__state AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_lookupset_map__cse__state")  }})
TgtMAP_CSE_STATELks AS (
	SELECT STAT_C,
		STAT_X
	FROM _cba__app_csel4_dev_lookupset_map__cse__state
)

SELECT * FROM TgtMAP_CSE_STATELks