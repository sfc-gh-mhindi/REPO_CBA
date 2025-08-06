{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_dataset_cse__cpl__bus__fee__margin__insertrejttablerecs', incremental_strategy='insert_overwrite', tags=['XfmAPPT_PDCT_FEATFrmPlFeePlMargin']) }}

SELECT
	PL_FEE_ID,
	PL_APP_PROD_ID,
	PL_FEE_PL_APP_PROD_ID,
	PL_MARGIN_PL_MARGIN_ID,
	PL_MARGIN_PL_FEE_ID,
	PL_MARGIN_MARGIN_REASON_CAT_ID,
	PL_FEE_FOUND_FLAG,
	PL_MARGIN_FOUND_FLAG,
	ETL_D,
	ORIG_ETL_D,
	EROR_C 
FROM {{ ref('SplitRejectTableRecs__InsertRejtTableRecs') }}