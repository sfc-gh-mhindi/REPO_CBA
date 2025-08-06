{{ config(materialized='incremental', alias='_cba__app_lpxs_lpxs__dev_dataset_appt__rel__3__i__cse__com__bus__app__prod__ccl__pl__app__prod__20071220', incremental_strategy='insert_overwrite', tags=['DltAPPT_RELFrmTMP_APPT_REL_3']) }}

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