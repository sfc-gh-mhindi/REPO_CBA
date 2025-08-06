{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_dataset_cse__chl__bus__int__rt__prc__prd__int__mrg__deleterejttablerecs', incremental_strategy='insert_overwrite', tags=['XfmAPPT_PDCT_FEATFrmHlProdIntMargin']) }}

SELECT
	HL_INT_RATE_ID 
FROM {{ ref('SplitRejectTableRecs__DeleteRejtTableRecs') }}