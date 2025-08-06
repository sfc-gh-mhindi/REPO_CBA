{{ config(materialized='view', tags=['XfmFaEnvisionEventFrmExt']) }}

WITH 
_cba__app_csel4_csel4dev_lookupset_map__cse__env__evnt__actv__type__fa__env__evnt__cat__id AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_lookupset_map__cse__env__evnt__actv__type__fa__env__evnt__cat__id")  }})
SrcMAP_CSE_ENV_EVNT_ACTV_TYPELks AS (
	SELECT FA_ENV_EVNT_CAT_ID,
		EVNT_ACTV_TYPE_C
	FROM _cba__app_csel4_csel4dev_lookupset_map__cse__env__evnt__actv__type__fa__env__evnt__cat__id
)

SELECT * FROM SrcMAP_CSE_ENV_EVNT_ACTV_TYPELks