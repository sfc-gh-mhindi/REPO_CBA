{{ config(materialized='view', tags=['XfmReasonSmCaseStateFrmExt']) }}

WITH 
_cba__app_csel4_csel4dev_lookupset_map__cse__sm__case__stus__sm__state__cat__id AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_lookupset_map__cse__sm__case__stus__sm__state__cat__id")  }})
SrcMAP_CSE_SM_CASE_STUSLks AS (
	SELECT SM_STATE_CAT_ID,
		STUS_C
	FROM _cba__app_csel4_csel4dev_lookupset_map__cse__sm__case__stus__sm__state__cat__id
)

SELECT * FROM SrcMAP_CSE_SM_CASE_STUSLks