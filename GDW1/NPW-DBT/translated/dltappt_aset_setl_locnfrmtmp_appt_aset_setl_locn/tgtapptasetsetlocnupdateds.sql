{{ config(materialized='incremental', alias='_cba__app_csel4_dev_dataset_appt__aset__setl__locn__u__cse__chl__bus__hlm__app__sec__20100616', incremental_strategy='insert_overwrite', tags=['DltAPPT_ASET_SETL_LOCNFrmTMP_APPT_ASET_SETL_LOCN']) }}

SELECT
	APPT_I,
	ASET_I,
	EFFT_D,
	SRCE_SYST_C,
	EXPY_D,
	pros_key_expy_i 
FROM {{ ref('XfmCheckDeltaAction__OutTgtApptAsetSetlLocnUpdateDS') }}