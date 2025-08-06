{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_dataset_cse__com__bus__reason__sm__case__state__merge__20061016', incremental_strategy='insert_overwrite', tags=['MergeReasonSmCaseState']) }}

SELECT
	SM_CASE_STATE_ID,
	SCS_SM_CASE_ID,
	SCS_SM_STATE_CAT_ID,
	SCS_START_DATE,
	SCS_END_DATE,
	SCS_CREATED_BY_STAFF_NUMBER,
	SCS_STATE_CAUSED_BY_ACTION_ID,
	SCSR_SM_CASE_STATE_REASON_ID,
	SCSR_SM_REASON_CAT_ID,
	SM_CASE_STATE_REAS_FOUND_FLAG,
	SM_CASE_STATE_FOUND_FLAG 
FROM {{ ref('XfmNoOp') }}