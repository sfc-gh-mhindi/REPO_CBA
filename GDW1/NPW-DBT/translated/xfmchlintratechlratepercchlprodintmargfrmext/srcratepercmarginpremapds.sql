{{ config(materialized='view', tags=['XfmChlIntRateChlRatePercChlProdIntMargFrmExt']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_cse__chl__bus__int__rt__perc__prod__int__marg__premap AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_cse__chl__bus__int__rt__perc__prod__int__marg__premap")  }})
SrcRatePercMarginPremapDS AS (
	SELECT HL_INT_RATE_ID,
		RATE_HL_INT_RATE_ID,
		RATE_HL_APP_PROD_ID,
		RATE_CASS_INT_RATE_TYPE_CODE,
		RATE_RATE_SEQUENCE,
		RATE_DURATION,
		PERC_HL_INT_RATE_ID,
		PERC_SCHEDULE_RATE_TYPE,
		PERC_RATE_PERCENT_1,
		PERC_RATE_PERCENT_2,
		MARG_HL_PROD_INT_MARGIN_ID,
		MARG_HL_INT_RATE_ID,
		MARG_HL_PROD_INT_MARGIN_CAT_ID,
		MARG_MARGIN_TYPE,
		MARG_MARGIN_DESC,
		MARG_MARGIN_CODE,
		MARG_MARGIN_AMOUNT,
		MARG_ADJ_AMT,
		RATE_FOUND_FLAG,
		PERC_FOUND_FLAG,
		MARG_FOUND_FLAG,
		ORIG_ETL_D
	FROM _cba__app_csel4_csel4dev_dataset_cse__chl__bus__int__rt__perc__prod__int__marg__premap
)

SELECT * FROM SrcRatePercMarginPremapDS