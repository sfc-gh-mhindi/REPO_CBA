{{ config(materialized='view', tags=['ExtCclBusApp']) }}

WITH 
_cba__app_csel4_csel4dev_inprocess_cse__ccl__bus__app__cse__ccl__bus__app__20060616 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_inprocess_cse__ccl__bus__app__cse__ccl__bus__app__20060616")  }})
SrcCclAppSeq AS (
	SELECT RECORD_TYPE,
		MOD_TIMESTAMP,
		CCL_APP_ID,
		CCL_APP_CAT_ID,
		CCL_FORM_CAT_ID,
		TOTAL_PERSONAL_FAC_AMT,
		TOTAL_EQUIPMENTFINANCE_FAC_AMT,
		TOTAL_COMMERCIAL_FAC_AMT,
		TOPUP_APP_ID,
		AF_PRIMARY_INDUSTRY_ID,
		AD_TUC_AMT,
		COMMISSION_AMT,
		BROKER_REFERAL_FLAG,
		CARNELL_EXPOSURE_AMT,
		CARNELL_EXPOSURE_AMT_DATE,
		CARNELL_OVERRIDE_COV_ASSESSMNT,
		CARNELL_OVERRIDE_REASON_CAT_ID,
		CARNELL_SHORT_DEFAULT_OVERRIDE,
		DUMMY
	FROM _cba__app_csel4_csel4dev_inprocess_cse__ccl__bus__app__cse__ccl__bus__app__20060616
)

SELECT * FROM SrcCclAppSeq