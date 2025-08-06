{{ config(materialized='view', tags=['ExtCSE_CCL_BUS_APP_FEE']) }}

WITH 
_cba__app_csel4_csel4dev_inprocess_cse__ccl__bus__app__fee__cse__ccl__bus__app__fee__20060616 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_inprocess_cse__ccl__bus__app__fee__cse__ccl__bus__app__fee__20060616")  }})
SrcCCL_APP_FEESeq AS (
	SELECT RECORD_TYPE,
		MOD_TIMESTAMP,
		CCL_APP_FEE_ID,
		CCL_APP_FEE_CCL_APP_ID,
		CCL_APP_FEE_CCL_APP_PROD_ID,
		CCL_APP_FEE_CHARGE_AMT,
		CCL_APP_FEE_CHARGE_DATE,
		CCL_APP_FEE_CONCESSION_FLAG,
		CCL_APP_FEE_CONCESSION_REASON,
		CCL_APP_FEE_OVERRIDE_FEE_PCT,
		CCL_APP_FEE_OVERRIDE_FEE_PCT_FREQ,
		CCL_APP_FEE_FEE_REPAYMENT_FREQ,
		CCL_FEE_TYPE_CAT_ID,
		CCL_APP_FEE_CHARGE_EXTERNAL_FLAG,
		DUMMY
	FROM _cba__app_csel4_csel4dev_inprocess_cse__ccl__bus__app__fee__cse__ccl__bus__app__fee__20060616
)

SELECT * FROM SrcCCL_APP_FEESeq