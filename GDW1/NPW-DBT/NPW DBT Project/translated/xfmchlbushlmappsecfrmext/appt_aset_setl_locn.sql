{{ config(materialized='incremental', alias='_cba__app_csel4_dev_dataset_tmp__cse__chl__bus__hlm__app__sec__appt__aset__setl__locn', incremental_strategy='insert_overwrite', tags=['XfmChlBusHlmAppSecFrmExt']) }}

SELECT
	APPT_I,
	ASET_I,
	SRCE_SYST_C,
	FRWD_DOCU_C,
	SETL_LOCN_X,
	SETL_CMMT_X,
	EFFT_D,
	EXPY_D,
	RUN_STRM 
FROM {{ ref('Xfm__ToApptAsetSetlLocn') }}