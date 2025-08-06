{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_dataset_appt__feat__u__cse__ccl__bus__app__fee__20060616', incremental_strategy='insert_overwrite', tags=['DltAPPT_FEATFrmTMP_APPT_FEAT']) }}

SELECT
	APPT_I,
	EFFT_D,
	SRCE_SYST_APPT_FEAT_I,
	EXPY_D,
	PROS_KEY_EXPY_I 
FROM {{ ref('XfmCheckDeltaAction__OutTgtApptFeatUpdateDS') }}