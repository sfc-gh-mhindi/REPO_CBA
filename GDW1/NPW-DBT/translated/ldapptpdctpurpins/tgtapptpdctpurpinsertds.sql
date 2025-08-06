{{ config(materialized='view', tags=['LdApptPdctPurpIns']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_appt__pdct__acct__i__cse__ccc__bus__app__prod__20060101 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_appt__pdct__acct__i__cse__ccc__bus__app__prod__20060101")  }})
TgtApptPdctPurpInsertDS AS (
	SELECT APPT_PDCT_I,
		EFFT_D,
		SRCE_SYST_APPT_PDCT_PURP_I,
		PURP_TYPE_C,
		PURP_CLAS_C,
		SRCE_SYST_C,
		PURP_A,
		CNCY_C,
		MAIN_PURP_F,
		EXPY_D,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		EROR_SEQN_I
	FROM _cba__app_csel4_csel4dev_dataset_appt__pdct__acct__i__cse__ccc__bus__app__prod__20060101
)

SELECT * FROM TgtApptPdctPurpInsertDS