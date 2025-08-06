{{ config(materialized='incremental', alias='_cba__app_csel4_dev_dataset_tmp__cse__chl__bus__hlm__app__sec__2__appt__aset', incremental_strategy='insert_overwrite', tags=['XfmChlBusHlmAppSecFrmExt']) }}

SELECT
	APPT_I,
	ASET_I,
	PRIM_SECU_F,
	EFFT_D,
	EXPY_D,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I,
	eror_seqn_i,
	ASET_SETL_REQD,
	RUN_STRM 
FROM {{ ref('Xfm__ToApptAset') }}