{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_dataset_cse__coi__bus__undtak__map__cse__sm__case', incremental_strategy='insert_overwrite', tags=['XfmFaUndertakingFrmExt']) }}

SELECT
	SM_CASE_ID,
	TARG_I,
	TARG_SUBJ 
FROM {{ ref('XfmBusinessRules__OutSmCaseDS') }}