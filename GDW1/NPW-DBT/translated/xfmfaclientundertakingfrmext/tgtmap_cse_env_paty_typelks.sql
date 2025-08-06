{{ config(materialized='view', tags=['XfmFaClientUndertakingFrmExt']) }}

WITH 
_cba__app_csel4_csel4dev_lookupset_map__cse__env__paty__type__fa__entity__cat__id AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_lookupset_map__cse__env__paty__type__fa__entity__cat__id")  }})
TgtMAP_CSE_ENV_PATY_TYPELks AS (
	SELECT FA_ENTITY_CAT_ID,
		PATY_TYPE_C
	FROM _cba__app_csel4_csel4dev_lookupset_map__cse__env__paty__type__fa__entity__cat__id
)

SELECT * FROM TgtMAP_CSE_ENV_PATY_TYPELks