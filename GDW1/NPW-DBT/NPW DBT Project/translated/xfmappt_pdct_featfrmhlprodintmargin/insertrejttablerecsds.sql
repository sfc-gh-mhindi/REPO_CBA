{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_dataset_cse__chl__bus__int__rt__prc__prd__int__mrg__insertrejttablerecs', incremental_strategy='insert_overwrite', tags=['XfmAPPT_PDCT_FEATFrmHlProdIntMargin']) }}

SELECT
	HL_INT_RATE_ID,
	MARG_HL_PROD_INT_MARGIN_ID,
	MARG_HL_INT_RATE_ID,
	MARG_HL_PROD_INT_MARGIN_CAT_ID,
	RATE_FOUND_FLAG,
	PERC_FOUND_FLAG,
	MARG_FOUND_FLAG,
	ETL_D,
	ORIG_ETL_D,
	EROR_C 
FROM {{ ref('SplitRejectTableRecs__InsertRejtTableRecs') }}