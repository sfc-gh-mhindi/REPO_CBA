{{ config(materialized='incremental', alias='_cba__app_csel4_dev_dataset_cse__com__bus__ccl__chl__com__app__map__cse__sm__case', incremental_strategy='insert_overwrite', tags=['XfmAppCclAppChlAppFrmExt']) }}

SELECT
	SM_CASE_ID,
	TARG_I,
	TARG_SUBJ 
FROM {{ ref('XfmBusinessRules__OutMapCseSmCaseDS') }}