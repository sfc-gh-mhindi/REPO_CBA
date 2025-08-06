{{ config(materialized='view', tags=['LdApptIns']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_appt__i__cse__com__bus__app__ccl__app__20060101 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_appt__i__cse__com__bus__app__ccl__app__20060101")  }})
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
		PROS_KEY_EFFT_I,
		EROR_SEQN_I
	FROM _cba__app_csel4_csel4dev_dataset_appt__i__cse__com__bus__app__ccl__app__20060101
)

SELECT * FROM TgtApptInsertDS