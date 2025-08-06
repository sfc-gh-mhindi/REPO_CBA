{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_dataset_appt__feat__u__cse__ccl__bus__app__fee__20060616', incremental_strategy='insert_overwrite', tags=['DltAPPT_FEAT_SHLFrmTMP_APPT_FEAT']) }}

SELECT
	APPT_I,
	EFFT_D,
	FEAT_I,
	SRCE_SYST_APPT_FEAT_I,
	EXPY_D,
	PROS_KEY_EXPY_I 
FROM {{ ref('Funnel_72') }}