{{ config(materialized='incremental', alias='_cba__app_mme_dev_dataset_appt__stus__i__cse__com__bus__sm__case__state__20090123', incremental_strategy='insert_overwrite', tags=['DltAPPT_STUSFrmTMP_APPT_STUS_CatchUp']) }}

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
	EROR_SEQN_I 
FROM {{ ref('XfmCheckDeltaAction__OutTgtApptStusInsertDS') }}