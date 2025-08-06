{{ config(materialized='incremental', alias='_cba__app_csel4_dev_dataset_appt__docu__i__cse__chl__bus__app__20100920', incremental_strategy='insert_overwrite', tags=['DltAPPT_DOCUFrmTMP_WIM_BUS']) }}

SELECT
	APPT_I,
	DOCU_C,
	SRCE_SYST_C,
	DOCU_VERS_C,
	EFFT_D,
	EXPY_D,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I,
	ROW_SECU_ACCS_C 
FROM {{ ref('Trnsfrmr_Appt_Docu') }}