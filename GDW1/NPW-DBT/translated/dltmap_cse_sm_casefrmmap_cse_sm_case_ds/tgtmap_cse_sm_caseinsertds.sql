{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_dataset_map__cse__sm__case__i__cse__com__bus__ccl__chl__com__app__20060101', incremental_strategy='insert_overwrite', tags=['DltMAP_CSE_SM_CASEFrmMAP_CSE_SM_CASE_DS']) }}

SELECT
	SM_CASE_ID,
	TARG_I,
	TARG_SUBJ,
	EFFT_D 
FROM {{ ref('XfmAddEfftDate') }}