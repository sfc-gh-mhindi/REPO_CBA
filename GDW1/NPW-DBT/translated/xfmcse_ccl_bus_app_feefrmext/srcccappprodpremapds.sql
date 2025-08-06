{{ config(materialized='view', tags=['XfmCSE_CCL_BUS_APP_FEEFrmExt']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_cse__ccl__bus__app__fee__premap AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_cse__ccl__bus__app__fee__premap")  }})
SrcCCAppProdPremapDS AS (
	SELECT CCL_APP_FEE_ID,
		CCL_APP_FEE_CCL_APP_ID,
		CCL_APP_FEE_CCL_APP_PROD_ID,
		CCL_APP_FEE_CHARGE_AMT,
		CCL_APP_FEE_CHARGE_DATE,
		CCL_APP_FEE_CONCESSION_FLAG,
		CCL_APP_FEE_CONCESSION_REASON,
		CCL_APP_FEE_OVERRIDE_FEE_PCT,
		CCL_APP_FEE_OVERRIDE_FEE_PCT_FREQ,
		CCL_APP_FEE_FEE_REPAYMENT_FREQ,
		CCL_APP_FEE_TYPE_CAT_ID,
		CCL_APP_FEE_CHARGE_EXTERNAL_FLAG,
		CCL_FEE_TYPE_CAT_ID,
		CCL_FEE_TYPE_CAT_DEFAULT_AMT,
		CCL_FEE_TYPE_CAT_DEFAULT_FEE_PCT,
		CCL_FEE_TYPE_CAT_FEE_TYPE_DESC,
		ORIG_ETL_D
	FROM _cba__app_csel4_csel4dev_dataset_cse__ccl__bus__app__fee__premap
)

SELECT * FROM SrcCCAppProdPremapDS