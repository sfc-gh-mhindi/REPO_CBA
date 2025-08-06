{{ config(materialized='view', tags=['MergeAppProdCclAppProdPlAppProd']) }}

WITH 
_cba__app_csel4_sit_inprocess_cse__clp__bus__app__prod__cse__com__bus__app__prod__ccl__pl__app__prod__20120423 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_sit_inprocess_cse__clp__bus__app__prod__cse__com__bus__app__prod__ccl__pl__app__prod__20120423")  }})
SrcClpAppProdSeq AS (
	SELECT CLP_RECORD_TYPE,
		MOD_TIMESTAMP,
		CLP_APP_PROD_ID,
		CAMPAIGN_CAT_ID,
		LOAN_AMT,
		INSURED_AMT,
		LOAN_APP_PROD_ID,
		LOAN_ASSET_LIABILITY_ID,
		CONTRACT_NO,
		PAYMENT_ID,
		CLIENT_ENGAGEMENT_CAT_ID,
		CLP_JOB_FAMILY_CAT_ID,
		CLIENT_STAFF_OU_ID,
		QUESTION_SUBMIT_DATE,
		NAVIGATION_CLIENT_ID,
		CLP_SUM_INSURED_CAT_ID,
		MOD_USER_ID,
		OU_SNAP_ID,
		CLP_DUMMY
	FROM _cba__app_csel4_sit_inprocess_cse__clp__bus__app__prod__cse__com__bus__app__prod__ccl__pl__app__prod__20120423
)

SELECT * FROM SrcClpAppProdSeq