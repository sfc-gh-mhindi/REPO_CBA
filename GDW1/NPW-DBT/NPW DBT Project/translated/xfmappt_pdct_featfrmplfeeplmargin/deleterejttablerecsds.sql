{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_dataset_cse__cpl__bus__fee__margin__deleterejttablerecs', incremental_strategy='insert_overwrite', tags=['XfmAPPT_PDCT_FEATFrmPlFeePlMargin']) }}

SELECT
	PL_FEE_ID 
FROM {{ ref('SplitRejectTableRecs__DeleteRejtTableRecs') }}