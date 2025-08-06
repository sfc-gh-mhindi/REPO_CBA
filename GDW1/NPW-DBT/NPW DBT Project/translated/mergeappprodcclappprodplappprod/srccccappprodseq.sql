{{ config(materialized='view', tags=['MergeAppProdCclAppProdPlAppProd']) }}

WITH 
_cba__app_csel4_sit_inprocess_cse__ccc__bus__app__prod__cse__com__bus__app__prod__ccl__pl__app__prod__20120423 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_sit_inprocess_cse__ccc__bus__app__prod__cse__com__bus__app__prod__ccl__pl__app__prod__20120423")  }})
SrcCccAppprodSeq AS (
	SELECT RECORD_TYPE,
		MOD_TIMESTAMP,
		CC_APP_PROD_ID,
		REQUESTED_LIMIT_AMT,
		CC_INTEREST_OPT_CAT_ID,
		CBA_HOMELOAN_NO,
		ABN,
		BUSINESS_NAME,
		READ_COSTS_AND_RISKS_FLAG,
		ACCEPTS_COSTS_AND_RISKS_DATE,
		PREV_APPRV_AMOUNT,
		DUMMY
	FROM _cba__app_csel4_sit_inprocess_cse__ccc__bus__app__prod__cse__com__bus__app__prod__ccl__pl__app__prod__20120423
)

SELECT * FROM SrcCccAppprodSeq