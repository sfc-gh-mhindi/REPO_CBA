{{ config(materialized='view', tags=['ExtPL_APP_PROD_PURP']) }}

WITH 
_cba__app_csel4_csel4dev_inprocess_cse__cpl__bus__app__prod__purp__cse__cpl__bus__app__prod__purp__20060616 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_inprocess_cse__cpl__bus__app__prod__purp__cse__cpl__bus__app__prod__purp__20060616")  }})
SrcPlAppProdPurpSeq AS (
	SELECT RECORD_TYPE,
		MOD_TIMESTAMP,
		PL_APP_PROD_PURP_ID,
		PL_APP_PROD_PURP_CAT_ID,
		AMT,
		PL_APP_PROD_ID,
		DUMMY
	FROM _cba__app_csel4_csel4dev_inprocess_cse__cpl__bus__app__prod__purp__cse__cpl__bus__app__prod__purp__20060616
)

SELECT * FROM SrcPlAppProdPurpSeq