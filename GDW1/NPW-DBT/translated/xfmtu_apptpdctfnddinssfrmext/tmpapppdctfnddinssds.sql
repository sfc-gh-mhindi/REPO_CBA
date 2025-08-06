{{ config(materialized='incremental', alias='_cba__app_hlt_dev_dataset_tmp__cse__chl__bus__tu__app__fund__det__appt__pdct__fndd__inss', incremental_strategy='insert_overwrite', tags=['XfmTu_ApptPdctFnddInssFrmExt']) }}

SELECT
	APPT_PDCT_I,
	APPT_PDCT_FNDD_I,
	APPT_PDCT_FNDD_METH_I,
	EFFT_D,
	FNDD_INSS_METH_C,
	SRCE_SYST_FNDD_I,
	SRCE_SYST_FNDD_METH_I,
	SRCE_SYST_C,
	FNDD_D,
	FNDD_A,
	PDCT_SYST_ACCT_N,
	CMPE_I,
	CMPE_ACCT_BSB_N,
	CMPE_ACCT_N,
	EXPY_D,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I,
	EROR_SEQN_I,
	RUN_STREAM 
FROM {{ ref('XfmBusinessRules__OutTmpAppPdctFnddInssDS') }}