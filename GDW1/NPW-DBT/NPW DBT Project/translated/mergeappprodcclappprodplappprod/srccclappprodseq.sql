{{ config(materialized='view', tags=['MergeAppProdCclAppProdPlAppProd']) }}

WITH 
_cba__app_csel4_sit_inprocess_cse__ccl__bus__app__prod__cse__com__bus__app__prod__ccl__pl__app__prod__20120423 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_sit_inprocess_cse__ccl__bus__app__prod__cse__com__bus__app__prod__ccl__pl__app__prod__20120423")  }})
SrcCclAppProdSeq AS (
	SELECT CCL_RECORD_TYPE,
		CCL_MOD_TIMESTAMP,
		CCL_CCL_APP_PROD_ID,
		CCL_PARENT_CCL_APP_PROD_ID,
		CCL_ACCOUNT_NUMBER,
		CCL_ACTUAL_RATE,
		CCL_BALLOON_PAY_AMT,
		CCL_BASIS_TYPE_CAT_ID,
		CCL_INTEREST_RATE_MARGIN,
		CCL_LOAN_TERM_MTH_NUM,
		CCL_CURRENT_LIMIT,
		CCL_INC_DEC_AMT,
		CCL_POST_APP_AMT,
		CCL_NEW_PROD_FLAG,
		CCL_INDEX_RATE,
		CCL_INTEREST_ADV_FLAG,
		CCL_INTERESTONLY_TERM_MTH_NUM,
		CCL_CAPPED_TERM_MTH_NUM,
		CCL_CAPPED_RATE,
		CCL_CRIS_PRODUCT_ID,
		CCL_CCL_LOAN_PURPOSE_ID,
		CCL_NEW_PROD_SUBTYPE_FLAG,
		CCL_TEMP_EXCESS_MTHS,
		CCL_LOAN_PURPOSE_CLASS_CODE,
		CCL_CCL_INTEREST_RATE_CAT_ID,
		CCL_CRR,
		CCL_AD_TUC_INC_AMT,
		CCL_CCL_RATE_TYPE_CAT_ID,
		CCL_LINK_CCL_APP_ID,
		CCL_LINK_CCL_APP_PROD_ID,
		CCL_CHL_LMI_AMT,
		CCL_USED_IN_EXPOSURE_VIEW_FLAG,
		CCL_CCL_APP_BROKER_CIF_CODE,
		CCL_APP_ID,
		CCL_FIXED_INT_START_DATE,
		CCL_CHILD_PDCT_LEVEL_CAT_ID,
		CCL_PARENT_PDCT_LEVEL_CAT_ID,
		CCL_DUMMY
	FROM _cba__app_csel4_sit_inprocess_cse__ccl__bus__app__prod__cse__com__bus__app__prod__ccl__pl__app__prod__20120423
)

SELECT * FROM SrcCclAppProdSeq