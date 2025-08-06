{{ config(materialized='view', tags=['ExtComBusSmCase']) }}

WITH 
_cba__app_csel4_csel4dev_inprocess_cse__com__bus__sm__case__cse__com__bus__sm__case__20060616 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_inprocess_cse__com__bus__sm__case__cse__com__bus__sm__case__20060616")  }})
SrcComBusSmCaseSeq AS (
	SELECT RECORD_TYPE,
		MOD_TIMESTAMP,
		SM_CASE_ID,
		CREATED_TIMESTAMP,
		WIM_PROCESS_ID,
		DUMMY
	FROM _cba__app_csel4_csel4dev_inprocess_cse__com__bus__sm__case__cse__com__bus__sm__case__20060616
)

SELECT * FROM SrcComBusSmCaseSeq