{{ config(materialized='view', tags=['MergeAppCclAppChlApp']) }}

WITH 
_cba__app_csel4_dev_inprocess_cse__ccl__bus__app__cse__com__bus__ccl__chl__com__app__20100614 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_inprocess_cse__ccl__bus__app__cse__com__bus__ccl__chl__com__app__20100614")  }})
SrcCclBusApp AS (
	SELECT CCL_APP_RECORD_TYPE,
		CCL_APP_MOD_TIMESTAMP,
		CCL_APP_CCL_APP_ID,
		CCL_APP_CCL_APP_CAT_ID,
		CCL_APP_CCL_FORM_CAT_ID,
		CCL_APP_TOT_PERSONAL_FAC_AMT,
		CCL_APP_TOT_EQUIPFIN_FAC_AMT,
		CCL_APP_TOT_COMMERCIAL_FAC_AMT,
		CCL_APP_TOPUP_APP_ID,
		CCL_APP_AF_PRIMARY_INDUSTRY_ID,
		CCL_APP_AD_TUC_AMT,
		CCL_APP_COMMISSION_AMT,
		CCL_APP_BROKER_REFERAL_FLAG,
		CCL_APP_DUMMY
	FROM _cba__app_csel4_dev_inprocess_cse__ccl__bus__app__cse__com__bus__ccl__chl__com__app__20100614
)

SELECT * FROM SrcCclBusApp