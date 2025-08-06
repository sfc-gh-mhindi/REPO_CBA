{{ config(materialized='view', tags=['LdTMP_APPT_PDCTFrmXfm']) }}

WITH 
_cba__app_lpxs_lpxs__dev_dataset_tmp__cse__com__bus__app__prod__ccl__pl__app__prod__appt__pdct AS (
	SELECT
	*
	FROM {{ source("","_cba__app_lpxs_lpxs__dev_dataset_tmp__cse__com__bus__app__prod__ccl__pl__app__prod__appt__pdct")  }})
SrcApptPdctDS AS (
	SELECT APPT_PDCT_I,
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
		RUN_STRM,
		APPT_PDCT_CATG_C,
		ASES_D,
		APPT_PDCT_DURT_C
	FROM _cba__app_lpxs_lpxs__dev_dataset_tmp__cse__com__bus__app__prod__ccl__pl__app__prod__appt__pdct
)

SELECT * FROM SrcApptPdctDS