{{ config(materialized='incremental', alias='_cba__app_csel4_dev_dataset_appt__aset__u__cse__chl__bus__hlm__app__sec__20100616', incremental_strategy='insert_overwrite', tags=['DltAPPT_ASETFrmTMP_APPT_ASET1']) }}

SELECT
	APPT_I,
	ASET_I,
	EFFT_D,
	EXPY_D,
	pros_key_expy_i 
FROM {{ ref('XfmCheckDeltaAction__OutTgtApptFeatUpdateDS') }}