{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_dataset_cse__chl__bus__fee__disc__fee__deleterejttablerecs', incremental_strategy='insert_overwrite', tags=['XfmAPPT_PDCT_FEATFrmHlFeeDiscount']) }}

SELECT
	HL_FEE_ID 
FROM {{ ref('SplitRejectTableRecs__DeleteRejtTableRecs') }}