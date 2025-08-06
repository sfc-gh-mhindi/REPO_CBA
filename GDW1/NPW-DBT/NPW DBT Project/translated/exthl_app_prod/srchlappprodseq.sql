{{ config(materialized='view', tags=['ExtHL_APP_PROD']) }}

WITH 
_cba__app_mme_dev_inprocess_cse__chl__bus__app__prod__cse__chl__bus__app__prod__20081016 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_mme_dev_inprocess_cse__chl__bus__app__prod__cse__chl__bus__app__prod__20081016")  }})
SrcHlAppProdSeq AS (
	SELECT RECORD_TYPE,
		MOD_TIMESTAMP,
		HL_APP_PROD_ID,
		PARENT_HL_APP_PROD_ID,
		HL_REPAYMENT_PERIOD_CAT_ID,
		AMOUNT,
		LOAN_TERM_MONTHS,
		ACCOUNT_NUMBER,
		TOTAL_LOAN_AMOUNT,
		HLS_FLAG,
		GDW_UPDATED_LDP_PAID_ON_AMOUNT,
		DUMMY
	FROM _cba__app_mme_dev_inprocess_cse__chl__bus__app__prod__cse__chl__bus__app__prod__20081016
)

SELECT * FROM SrcHlAppProdSeq