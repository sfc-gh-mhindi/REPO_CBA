{{ config(materialized='view', tags=['XfmFaUndertakingFrmExt']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_cse__coi__bus__undtak__premap AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_cse__coi__bus__undtak__premap")  }})
SrcFAUndertakingDS AS (
	SELECT FA_UNDERTAKING_ID,
		PLANNING_GROUP_NAME,
		COIN_ADVICE_GROUP_ID,
		ADVICE_GROUP_CORRELATION_ID,
		CREATED_DATE,
		CREATED_BY_STAFF_NUMBER,
		SM_CASE_ID,
		ORIG_ETL_D
	FROM _cba__app_csel4_csel4dev_dataset_cse__coi__bus__undtak__premap
)

SELECT * FROM SrcFAUndertakingDS