{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_dataset_cse__cpl__bus__int__rate__amt__margin__insertrejttablerecs', incremental_strategy='insert_overwrite', tags=['XfmAPPT_PDCT_FEATFrmPlMargin']) }}

SELECT
	PL_INT_RATE_ID,
	PL_MARGIN_PL_MARGIN_ID,
	PL_MARGIN_PL_INT_RATE_ID,
	PL_MARGIN_MARGIN_RESN_CAT_ID,
	PL_MARGIN_FOUND_FLAG,
	PL_INT_RATE_FOUND_FLAG,
	PL_INT_RATE_AMT_FOUND_FLAG,
	ETL_D,
	ORIG_ETL_D,
	EROR_C 
FROM {{ ref('SplitRejectTableRecs__InsertRejtTableRecs') }}