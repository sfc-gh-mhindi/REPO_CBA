{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_dataset_tmp__cse__com__bus__sm__case__state__appt__stus', incremental_strategy='insert_overwrite', tags=['XfmSmCaseStateFrmExt']) }}

SELECT
	APPT_I,
	STUS_C,
	STRT_S,
	STRT_D,
	STRT_T,
	END_D,
	END_T,
	END_S,
	EMPL_I,
	EFFT_D,
	EXPY_D,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I,
	EROR_SEQN_I,
	RUN_STRM 
FROM {{ ref('XfmBusinessRules__OutTmpApptStusDS') }}