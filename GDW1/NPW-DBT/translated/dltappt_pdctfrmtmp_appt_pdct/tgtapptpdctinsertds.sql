{{ config(materialized='incremental', alias='_cba__app_csel4_dev_dataset_appt__pdct__i__cse__com__bus__app__prod__ccl__pl__app__prod__20170306', incremental_strategy='insert_overwrite', tags=['DltAPPT_PDCTFrmTMP_APPT_PDCT']) }}

SELECT
	APPT_PDCT_I,
	DEBT_ABN_X,
	DEBT_BUSN_M,
	SMPL_APPT_F,
	JOB_COMM_CATG_C,
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
	BROK_PATY_I,
	COPY_FROM_OTHR_APPT_F,
	EFFT_D,
	EXPY_D,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I,
	EROR_SEQN_I,
	APPT_PDCT_CATG_C,
	APPT_PDCT_DURT_C,
	ASES_D 
FROM {{ ref('XfmCheckDeltaAction__OutTgtApptPdctInsertDS') }}