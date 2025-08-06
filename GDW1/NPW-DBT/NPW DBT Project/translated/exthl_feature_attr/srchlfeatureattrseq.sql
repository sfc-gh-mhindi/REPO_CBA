{{ config(materialized='view', tags=['ExtHL_FEATURE_ATTR']) }}

WITH 
_cba__app_csel4_csel4dev_inprocess_cse__chl__bus__feat__attr__cse__chl__bus__feat__attr__20060616 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_inprocess_cse__chl__bus__feat__attr__cse__chl__bus__feat__attr__20060616")  }})
SrcHlFeatureAttrSeq AS (
	SELECT RECORD_TYPE,
		MOD_TIMESTAMP,
		HL_FEATURE_ATTR_ID,
		HL_FEATURE_TERM,
		HL_FEATURE_AMOUNT,
		HL_FEATURE_BALANCE,
		HL_FEATURE_FEE,
		HL_FEATURE_SPEC_REPAY,
		HL_FEATURE_EST_INT_AMT,
		HL_FEATURE_DATE,
		HL_FEATURE_COMMENT,
		HL_FEATURE_CAT_ID,
		HL_APP_PROD_ID,
		DUMMY
	FROM _cba__app_csel4_csel4dev_inprocess_cse__chl__bus__feat__attr__cse__chl__bus__feat__attr__20060616
)

SELECT * FROM SrcHlFeatureAttrSeq