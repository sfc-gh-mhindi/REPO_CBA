{{ config(materialized='incremental', alias='_cba__app_hlt_sit_dataset_paty__appt__i__cse__ccl__bus__app__client__20080915', incremental_strategy='insert_overwrite', tags=['DltAPPT_PATYFrmTMP_APPT_PATY']) }}

SELECT
	APPT_I,
	PATY_I,
	EFFT_D,
	EXPY_D,
	REL_C,
	REL_REAS_C,
	REL_STUS_C,
	REL_LEVL_C,
	SRCE_SYST_C,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I,
	EROR_SEQN_I 
FROM {{ ref('XfmCheckDeltaAction__OutTgtPatyApptInsertDS') }}