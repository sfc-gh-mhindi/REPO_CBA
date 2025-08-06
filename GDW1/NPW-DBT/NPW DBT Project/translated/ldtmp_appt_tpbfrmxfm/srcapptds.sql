{{ config(materialized='view', tags=['LdTMP_APPT_TPBFrmXfm']) }}

WITH 
_cba__app_hlt_dev_dataset_tmp__cse__com__bus__ccl__chl__com__app__appt AS (
	SELECT
	*
	FROM {{ source("","_cba__app_hlt_dev_dataset_tmp__cse__com__bus__ccl__chl__com__app__appt")  }})
SrcApptDS AS (
	SELECT APPT_I,
		APPT_C,
		APPT_FORM_C,
		APPT_QLFY_C,
		STUS_TRAK_I,
		APPT_ORIG_C,
		APPT_N,
		SRCE_SYST_C,
		SRCE_SYST_APPT_I,
		APPT_CRAT_D,
		RATE_SEEK_F,
		PROS_KEY_EFFT_I,
		EROR_SEQN_I,
		RUN_STRM,
		ORIG_APPT_SRCE_C,
		APPT_RECV_S,
		REL_MGR_STAT_C,
		APPT_RECV_D,
		APPT_RECV_T
	FROM _cba__app_hlt_dev_dataset_tmp__cse__com__bus__ccl__chl__com__app__appt
)

SELECT * FROM SrcApptDS