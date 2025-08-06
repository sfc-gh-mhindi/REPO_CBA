{{ config(materialized='view', tags=['MergeAppProdClpAppProd']) }}

WITH 
_cba__app_csel4_csel4__prd_inprocess_cse__clp__bus__app__prod__cse__clp__bus__appt__pdct__feat__20080405 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4__prd_inprocess_cse__clp__bus__app__prod__cse__clp__bus__appt__pdct__feat__20080405")  }})
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
	FROM _cba__app_csel4_csel4__prd_inprocess_cse__clp__bus__app__prod__cse__clp__bus__appt__pdct__feat__20080405
)

SELECT * FROM SrcClpAppProdSeq