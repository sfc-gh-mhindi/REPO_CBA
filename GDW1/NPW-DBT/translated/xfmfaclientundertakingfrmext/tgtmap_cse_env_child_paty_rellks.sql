{{ config(materialized='view', tags=['XfmFaClientUndertakingFrmExt']) }}

WITH 
_cba__app_csel4_csel4dev_lookupset_map__cse__env__child__paty__rel__fa__child__status__cat__id AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_lookupset_map__cse__env__child__paty__rel__fa__child__status__cat__id")  }})
TgtMAP_CSE_ENV_CHILD_PATY_RELLks AS (
	SELECT FA_CHILD_STATUS_CAT_ID,
		REL_C
	FROM _cba__app_csel4_csel4dev_lookupset_map__cse__env__child__paty__rel__fa__child__status__cat__id
)

SELECT * FROM TgtMAP_CSE_ENV_CHILD_PATY_RELLks