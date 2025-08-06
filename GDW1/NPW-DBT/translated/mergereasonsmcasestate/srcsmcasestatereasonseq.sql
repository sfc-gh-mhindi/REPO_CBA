{{ config(materialized='view', tags=['MergeReasonSmCaseState']) }}

WITH 
_cba__app_csel4_csel4dev_inprocess_cse__com__bus__sm__case__state__reason__cse__com__bus__reason__sm__case__state__20061016 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_inprocess_cse__com__bus__sm__case__state__reason__cse__com__bus__reason__sm__case__state__20061016")  }})
SrcSmCaseStateReasonSeq AS (
	SELECT SCSR_RECORD_TYPE,
		SCSR_MOD_TIMESTAMP,
		SCSR_SM_CASE_STATE_REASON_ID,
		SCSR_SM_REASON_CAT_ID,
		SM_CASE_STATE_ID,
		SCSR_DUMMY
	FROM _cba__app_csel4_csel4dev_inprocess_cse__com__bus__sm__case__state__reason__cse__com__bus__reason__sm__case__state__20061016
)

SELECT * FROM SrcSmCaseStateReasonSeq