{{ config(materialized='view', tags=['ExtHL_APP_PROD_PURPOSE']) }}

WITH 
_cba__app_csel4_csel4dev_inprocess_cse__chl__bus__app__prod__purp__cse__chl__bus__app__prod__purp__20060616 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_inprocess_cse__chl__bus__app__prod__purp__cse__chl__bus__app__prod__purp__20060616")  }})
SrcHlAppProdPurposeSeq AS (
	SELECT RECORD_TYPE,
		MOD_TIMESTAMP,
		HL_APP_PROD_PURPOSE_ID,
		HL_APP_PROD_ID,
		HL_LOAN_PURPOSE_CAT_ID,
		AMOUNT,
		MAIN_PURPOSE,
		DUMMY
	FROM _cba__app_csel4_csel4dev_inprocess_cse__chl__bus__app__prod__purp__cse__chl__bus__app__prod__purp__20060616
)

SELECT * FROM SrcHlAppProdPurposeSeq