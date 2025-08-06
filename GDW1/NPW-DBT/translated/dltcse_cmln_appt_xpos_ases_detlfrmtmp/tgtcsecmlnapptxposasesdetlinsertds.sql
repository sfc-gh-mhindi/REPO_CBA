{{ config(materialized='incremental', alias='_cba__app_csel4_dev_dataset_appt__pdct__cpgn__i__cse__ccc__bus__app__prod__20060616', incremental_strategy='insert_overwrite', tags=['DltCSE_CMLN_APPT_XPOS_ASES_DETLFrmTMP']) }}

SELECT
	APPT_I,
	XPOS_A,
	XPOS_AMT_D,
	OVRD_COVTS_ASES_F,
	CSE_CMLN_OVRD_REAS_CATG_C,
	SHRT_DFLT_OVRD_F,
	EFFT_D,
	EXPY_D,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I 
FROM {{ ref('xf_delta__ln_inserts') }}