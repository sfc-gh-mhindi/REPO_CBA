{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_dataset_appt__rel__2__i__cse__ccl__bus__hl__app__20060616', incremental_strategy='insert_overwrite', tags=['DltAPPT_RELFrmTMP_APPT_REL_2']) }}

SELECT
	APPT_I,
	RELD_APPT_I,
	REL_TYPE_C,
	EFFT_D,
	EXPY_D,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I,
	EROR_SEQN_I 
FROM {{ ref('XfmCheckDeltaAction__OutTgtApptRelInsertDS') }}