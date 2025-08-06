{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_dataset_cse__cpl__bus__int__rate__amt__margin__updaterejttablerecs', incremental_strategy='insert_overwrite', tags=['XfmAPPT_PDCT_FEATFrmPlMargin']) }}

SELECT
	PL_INT_RATE_ID,
	PL_MARGIN_MARGIN_AMT,
	PL_MARGIN_MARGIN_RESN_CAT_ID,
	PL_MARGIN_FOUND_FLAG,
	ETL_D 
FROM {{ ref('SplitRejectTableRecs__UpdateRejtTableRecs') }}