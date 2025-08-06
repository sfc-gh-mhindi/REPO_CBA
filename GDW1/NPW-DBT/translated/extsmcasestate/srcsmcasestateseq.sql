{{ config(materialized='view', tags=['ExtSmCaseState']) }}

WITH 
_cba__app_csel4_csel4dev_inprocess_cse__com__bus__sm__case__state__cse__com__bus__sm__case__state__20060616 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_inprocess_cse__com__bus__sm__case__state__cse__com__bus__sm__case__state__20060616")  }})
SrcSmCaseStateSeq AS (
	SELECT RECORD_TYPE,
		MOD_TIMESTAMP,
		SM_CASE_STATE_ID,
		SM_CASE_ID,
		SM_STATE_CAT_ID,
		START_DATE,
		END_DATE,
		CREATED_BY_STAFF_NUMBER,
		STATE_CAUSED_BY_ACTION_ID,
		DUMMY
	FROM _cba__app_csel4_csel4dev_inprocess_cse__com__bus__sm__case__state__cse__com__bus__sm__case__state__20060616
)

SELECT * FROM SrcSmCaseStateSeq