{{ config(materialized='view', tags=['ExtCclBusAppClient']) }}

WITH 
_cba__app_hlt_sit_inprocess_cse__ccl__bus__app__client__cse__ccl__bus__app__client__20080818 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_hlt_sit_inprocess_cse__ccl__bus__app__client__cse__ccl__bus__app__client__20080818")  }})
SrcCclAppSeq AS (
	SELECT RECORD_TYPE,
		MOD_TIMESTAMP,
		CCL_APP_CLIENT_ID,
		CCL_APP_CLIENT_VN,
		CCL_APP_ID,
		CIF_CODE,
		APPLICANT_FLAG,
		RELATED_PARTY_FLAG,
		RELATED_APP_CLIENT_ID,
		CCL_AD_CLIENT_POSITION_CAT_ID,
		PRIVACY_ACT_ACCEPT_FLAG,
		TIME_MNG_CURRENT_BUS_MTH,
		SOLE_TRADER_FLAG,
		AD_SENT_FLAG,
		TIME_IN_INDUSTRY_MTH,
		BFD_NONSTD_EXEC_CLAUSE,
		REGISTERED_BUS_NUM,
		RESIDENCY_STATUS_ID,
		FM_GUARANTOR_SEARCH_FLAG,
		AD_SEQUENCE_NO,
		CCL_FM_EXEC_METHOD_CAT_ID,
		MOD_CBA_STAFF_NO,
		MOD_CSE_USER_ID,
		DUMMY
	FROM _cba__app_hlt_sit_inprocess_cse__ccl__bus__app__client__cse__ccl__bus__app__client__20080818
)

SELECT * FROM SrcCclAppSeq