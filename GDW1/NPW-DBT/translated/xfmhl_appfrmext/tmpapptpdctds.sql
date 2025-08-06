{{ config(materialized='incremental', alias='_cba__app_pj__rapidresponseteam_csel4_dev_dataset_tmp__cse__chl__bus__app__appt__pdct', incremental_strategy='insert_overwrite', tags=['XfmHL_APPFrmExt']) }}

SELECT
	APPT_PDCT_I,
	APPT_QLFY_C,
	ACQR_TYPE_C,
	ACQR_ADHC_X,
	ACQR_SRCE_C,
	PDCT_N,
	APPT_I,
	SRCE_SYST_C,
	SRCE_SYST_APPT_PDCT_I,
	LOAN_FNDD_METH_C,
	NEW_ACCT_F,
	EFFT_D,
	BROK_PATY_I,
	COPY_FROM_OTHR_APPT_F,
	EXPY_D,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I,
	EROR_SEQN_I,
	RUN_STRM,
	APPT_PDCT_CATG_C,
	ASES_D,
	APPT_PDCT_DURT_C 
FROM {{ ref('DeDup_ApptPdct') }}