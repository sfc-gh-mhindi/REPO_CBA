{{ config(materialized='view', tags=['XfmChlBusTuAPPFrmExt']) }}

WITH 
_cba__app_hlt_dev_lookupset_map__cse__adrs__type AS (
	SELECT
	*
	FROM {{ source("","_cba__app_hlt_dev_lookupset_map__cse__adrs__type")  }})
TgtMAP_CSE_ADRS_TYPELks AS (
	SELECT ADRS_TYPE_ID,
		PYAD_TYPE_C
	FROM _cba__app_hlt_dev_lookupset_map__cse__adrs__type
)

SELECT * FROM TgtMAP_CSE_ADRS_TYPELks