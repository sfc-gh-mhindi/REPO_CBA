{{ config(materialized='view', tags=['XfmFaClientUndertakingFrmExt']) }}

WITH 
_cba__app_csel4_csel4dev_lookupset_map__cse__env__paty__rel__clnt__posn__clnt__reln__type__id AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_lookupset_map__cse__env__paty__rel__clnt__posn__clnt__reln__type__id")  }})
TgtMAP_CSE_ENV_PATY_RELLks AS (
	SELECT CLIENT_RELATIONSHIP_TYPE_ID,
		CLIENT_POSITION,
		REL_C
	FROM _cba__app_csel4_csel4dev_lookupset_map__cse__env__paty__rel__clnt__posn__clnt__reln__type__id
)

SELECT * FROM TgtMAP_CSE_ENV_PATY_RELLks