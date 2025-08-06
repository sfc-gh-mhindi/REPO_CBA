{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_dataset_cse__chl__bus__int__rt__prc__prd__int__mrg__updaterejttablerecs', incremental_strategy='insert_overwrite', tags=['XfmAPPT_PDCT_FEATFrmHlProdIntMargin']) }}

SELECT
	HL_INT_RATE_ID,
	MARG_HL_PROD_INT_MARGIN_CAT_ID,
	MARG_MARGIN_TYPE,
	MARG_MARGIN_DESC,
	MARG_MARGIN_CODE,
	MARG_MARGIN_AMOUNT,
	MARG_ADJ_AMT,
	MARG_FOUND_FLAG,
	ETL_D 
FROM {{ ref('SplitRejectTableRecs__UpdateRejtTableRecs') }}