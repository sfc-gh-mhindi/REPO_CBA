{{ config(materialized='view', tags=['MergeReasonSmCaseState']) }}

WITH 
_cba__app_csel4_csel4dev_inprocess_cse__com__bus__sm__case__state__cse__com__bus__reason__sm__case__state__20061016 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_inprocess_cse__com__bus__sm__case__state__cse__com__bus__reason__sm__case__state__20061016")  }})
SrcSmCaseStateSeq AS (
	SELECT SCS_RECORD_TYPE,
		SCS_MOD_TIMESTAMP,
		SM_CASE_STATE_ID,
		SCS_SM_CASE_ID,
		SCS_SM_STATE_CAT_ID,
		SCS_START_DATE,
		SCS_END_DATE,
		SCS_CREATED_BY_STAFF_NUMBER,
		SCS_STATE_CAUSED_BY_ACTION_ID,
		SCS_DUMMY
	FROM _cba__app_csel4_csel4dev_inprocess_cse__com__bus__sm__case__state__cse__com__bus__reason__sm__case__state__20061016
)

SELECT * FROM SrcSmCaseStateSeq