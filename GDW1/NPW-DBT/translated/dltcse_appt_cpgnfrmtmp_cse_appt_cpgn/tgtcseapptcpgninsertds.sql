{{ config(materialized='incremental', alias='_cba__app_csel4_dev_dataset_cse__appt__cpgn__i__cse__chl__bus__app__20060616', incremental_strategy='insert_overwrite', tags=['DltCSE_APPT_CPGNFrmTMP_CSE_APPT_CPGN']) }}

SELECT
	APPT_I,
	CSE_CPGN_CODE_X,
	EFFT_D,
	EXPY_D,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I,
	ROW_SECU_ACCS_C 
FROM {{ ref('xf_delta__ln_inserts') }}