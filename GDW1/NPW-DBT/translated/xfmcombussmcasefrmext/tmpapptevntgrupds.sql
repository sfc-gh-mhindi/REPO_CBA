{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_dataset_tmp__cse__com__bus__sm__case__appt__evnt__grup', incremental_strategy='insert_overwrite', tags=['XfmComBusSmCaseFrmExt']) }}

SELECT
	APPT_I,
	EVNT_GRUP_I,
	EFFT_D,
	EXPY_D,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I,
	EROR_SEQN_I,
	RUN_STRM 
FROM {{ ref('XfmBusinessRules') }}