{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_dataset_tmp__cse__com__bus__reason__sm__case__state__appt__stus__reas', incremental_strategy='insert_overwrite', tags=['XfmReasonSmCaseStateFrmExt']) }}

SELECT
	APPT_I,
	STUS_C,
	STUS_REAS_TYPE_C,
	STRT_S,
	END_S,
	EFFT_D,
	EXPY_D,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I,
	EROR_SEQN_I,
	RUN_STRM 
FROM {{ ref('XfmBusinessRules__OutTmpApptStusReasDS') }}