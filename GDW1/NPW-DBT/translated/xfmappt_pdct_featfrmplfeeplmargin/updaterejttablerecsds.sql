{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_dataset_cse__cpl__bus__fee__margin__updaterejttablerecs', incremental_strategy='insert_overwrite', tags=['XfmAPPT_PDCT_FEATFrmPlFeePlMargin']) }}

SELECT
	PL_FEE_ID,
	PL_MARGIN_MARGIN_AMT,
	PL_MARGIN_MARGIN_REASON_CAT_ID,
	PL_MARGIN_FOUND_FLAG,
	ETL_D 
FROM {{ ref('SplitRejectTableRecs__UpdateRejtTableRecs') }}