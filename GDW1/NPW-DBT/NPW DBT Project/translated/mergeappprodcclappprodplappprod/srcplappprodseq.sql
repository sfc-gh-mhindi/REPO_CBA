{{ config(materialized='view', tags=['MergeAppProdCclAppProdPlAppProd']) }}

WITH 
_cba__app_csel4_sit_inprocess_cse__cpl__bus__app__prod__cse__com__bus__app__prod__ccl__pl__app__prod__20120423 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_sit_inprocess_cse__cpl__bus__app__prod__cse__com__bus__app__prod__ccl__pl__app__prod__20120423")  }})
SrcPlAppProdSeq AS (
	SELECT CPL_RECORD_TYPE,
		CPL_MOD_TIMESTAMP,
		CPL_PL_APP_PROD_ID,
		CPL_PL_TARGET_CAT_ID,
		CPL_REPAY_APPROX_AMT,
		CPL_REPAY_FREQUENCY_ID,
		CPL_PL_APP_PROD_REPAY_CAT_ID,
		CPL_PL_PROD_TERM_CAT_ID,
		CPL_PL_CAMPAIGN_CAT_ID,
		CPL_AD_HOC_CAMPAIGN_DESC,
		CPL_CAR_SEEKER_FLAG,
		CPL_PL_PROD_TARGET_CAT_ID,
		CPL_PL_MARKETING_SOURCE_CAT_ID,
		CPL_HLS_ACCT_NO,
		CPL_TOTAL_INTEREST_AMT,
		CPL_APP_PROD_AMT,
		CPL_TP_BROKER_ID,
		CPL_TP_BROKER_FIRST_NAME,
		CPL_TP_BROKER_LAST_NAME,
		CPL_TP_BROKER_GROUP_CAT_ID,
		CPL_READ_COSTS_AND_RISKS_FLAG,
		CPL_ACCEPTS_COSTS_AND_RISKS_DATE,
		CPL_DUMMY
	FROM _cba__app_csel4_sit_inprocess_cse__cpl__bus__app__prod__cse__com__bus__app__prod__ccl__pl__app__prod__20120423
)

SELECT * FROM SrcPlAppProdSeq