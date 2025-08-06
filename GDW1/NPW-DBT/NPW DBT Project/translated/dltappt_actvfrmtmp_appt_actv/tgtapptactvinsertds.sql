{{ config(materialized='incremental', alias='_cba__app_csel4_dev_dataset_appt__actv__i__cse__chl__bus__app__20100915', incremental_strategy='insert_overwrite', tags=['DltAPPT_ACTVFrmTMP_APPT_ACTV']) }}

SELECT
	APPT_I,
	APPT_ACTV_TYPE_C,
	SRCE_SYST_C,
	APPT_ACTV_Q,
	EFFT_D,
	EXPY_D,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I,
	ROW_SECU_ACCS_C 
FROM {{ ref('XfmCheckDeltaAction__OutTgtApptActvInsertDS') }}