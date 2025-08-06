{{ config(materialized='incremental', alias='_cba__app_csel4_csel4__prd_dataset_tmp__cse__com__bus__ccl__chl__com__app__appt__aset', incremental_strategy='insert_overwrite', tags=['XfmChlBusPrtyAdrs']) }}

SELECT
	APPT_I,
	ASET_I,
	PRIM_SECU_F,
	EFFT_D,
	EXPY_D,
	EROR_SEQN_I,
	RUN_STRM,
	CHL_PRCP_SCUY_FLAG 
FROM {{ ref('XfmBusinessRules__OutApptAsetDS') }}