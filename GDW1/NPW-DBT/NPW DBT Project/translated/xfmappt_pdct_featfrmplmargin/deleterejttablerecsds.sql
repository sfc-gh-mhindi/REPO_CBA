{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_dataset_cse__cpl__bus__int__rate__amt__margin__deleterejttablerecs', incremental_strategy='insert_overwrite', tags=['XfmAPPT_PDCT_FEATFrmPlMargin']) }}

SELECT
	PL_INT_RATE_ID 
FROM {{ ref('SplitRejectTableRecs__DeleteRejtTableRecs') }}