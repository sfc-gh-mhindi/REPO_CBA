{{ config(materialized='view', tags=['LdApptInsert']) }}

WITH 
_cba__app_csel4_dev_dataset_appt__i__cse__com__cpo__bus__ncpr__clnt__20101010 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_dataset_appt__i__cse__com__cpo__bus__ncpr__clnt__20101010")  }})
TgtApptInsertDS AS (
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
		ORIG_APPT_SRCE_C,
		APPT_RECV_S,
		REL_MGR_STAT_C,
		APPT_RECV_D,
		APPT_RECV_T,
		PROS_KEY_EFFT_I,
		EROR_SEQN_I,
		APPT_ENTR_POIT_M
	FROM _cba__app_csel4_dev_dataset_appt__i__cse__com__cpo__bus__ncpr__clnt__20101010
)

SELECT * FROM TgtApptInsertDS