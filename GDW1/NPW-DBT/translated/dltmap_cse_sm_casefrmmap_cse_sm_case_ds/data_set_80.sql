{{ config(materialized='view', tags=['DltMAP_CSE_SM_CASEFrmMAP_CSE_SM_CASE_DS']) }}

WITH 
_cba__app_csel4_csel4dev_lookupset_map__cse__sm__case__sm__case__id__lkp AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_lookupset_map__cse__sm__case__sm__case__id__lkp")  }})
Data_Set_80 AS (
	SELECT SM_CASE_ID,
		TARG_I,
		TARG_SUBJ
	FROM _cba__app_csel4_csel4dev_lookupset_map__cse__sm__case__sm__case__id__lkp
)

SELECT * FROM Data_Set_80