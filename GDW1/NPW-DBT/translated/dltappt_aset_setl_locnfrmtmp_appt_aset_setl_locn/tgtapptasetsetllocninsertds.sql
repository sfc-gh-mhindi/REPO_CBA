{{ config(materialized='incremental', alias='_cba__app_csel4_dev_dataset_appt__aset__setl__locn__i__cse__chl__bus__hlm__app__sec__20100616', incremental_strategy='insert_overwrite', tags=['DltAPPT_ASET_SETL_LOCNFrmTMP_APPT_ASET_SETL_LOCN']) }}

SELECT
	APPT_I,
	ASET_I,
	SRCE_SYST_C,
	FRWD_DOCU_C,
	SETL_LOCN_X,
	SETL_CMMT_X,
	efft_d,
	expy_d,
	pros_key_efft_i,
	pros_key_expy_i 
FROM {{ ref('XfmCheckDeltaAction__OutTgtApptAsetSetlLocnInsertDS') }}