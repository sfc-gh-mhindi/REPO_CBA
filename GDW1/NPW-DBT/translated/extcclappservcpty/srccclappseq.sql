{{ config(materialized='view', tags=['ExtCclappServCpty']) }}

WITH 
_cba__app_hlt_sit_inprocess_cse__ccl__bus__app__servicetst__cse__ccl__bus__app__servicetst__20080914 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_hlt_sit_inprocess_cse__ccl__bus__app__servicetst__cse__ccl__bus__app__servicetst__20080914")  }})
SrcCclAppSeq AS (
	SELECT RECORD_TYPE,
		MOD_TIMESTAMP,
		CCL_APP_SERVICETST_ID,
		CCL_APP_SERVICETST_VN,
		CCL_APP_ID,
		SERVICETST_PASS_FLAG,
		SERVICETST_NAME,
		NET_INCOME_AMT,
		NET_COMMITMENT_AMT,
		NET_SURPLUS_AMT,
		COMMITMENT_LEVEL,
		SEPERATED_HOUSHOLD_NUM,
		NON_INVESTMENT_PROP_NUM,
		TOTAL_HOUSEHOLD_EXP_AMT,
		MOD_CBA_STAFF_NO,
		MOD_CSE_USER_ID
	FROM _cba__app_hlt_sit_inprocess_cse__ccl__bus__app__servicetst__cse__ccl__bus__app__servicetst__20080914
)

SELECT * FROM SrcCclAppSeq