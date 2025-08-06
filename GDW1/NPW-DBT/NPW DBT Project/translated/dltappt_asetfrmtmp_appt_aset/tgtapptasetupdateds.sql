{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_dataset_appt__feat__u__cse__ccl__bus__app__fee__20060616', incremental_strategy='insert_overwrite', tags=['DltAPPT_ASETFrmTMP_APPT_ASET']) }}

SELECT
	APPT_I,
	ASET_I,
	EFFT_D,
	EXPY_D,
	pros_key_expy_i 
FROM {{ ref('XfmCheckDeltaAction__OutTgtApptFeatUpdateDS') }}