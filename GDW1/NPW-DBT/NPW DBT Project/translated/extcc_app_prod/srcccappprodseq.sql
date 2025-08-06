{{ config(materialized='view', tags=['ExtCC_APP_PROD']) }}

WITH 
_cba__app_csel4_dev_inprocess_cse__ccc__bus__app__prod__cse__ccc__bus__app__prod__20101213 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_inprocess_cse__ccc__bus__app__prod__cse__ccc__bus__app__prod__20101213")  }})
SrcCCAppProdSeq AS (
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
		PRE_APPRV_AMOUNT,
		NTM_CAMPAIGN_ID,
		FRIES_CAMPAIGN_CODE,
		OAP_CAMPAIGN_CODE,
		DUMMY
	FROM _cba__app_csel4_dev_inprocess_cse__ccc__bus__app__prod__cse__ccc__bus__app__prod__20101213
)

SELECT * FROM SrcCCAppProdSeq