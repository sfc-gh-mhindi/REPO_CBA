{{ config(materialized='view', tags=['LdApptPdctRpayIns']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_appt__pdct__acct__i__cse__ccc__bus__app__prod__20060101 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_appt__pdct__acct__i__cse__ccc__bus__app__prod__20060101")  }})
TgtApptPdctRpayInsertDS AS (
	SELECT APPT_PDCT_I,
		RPAY_TYPE_C,
		EFFT_D,
		PAYT_FREQ_C,
		STRT_RPAY_D,
		EXPY_D,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		EROR_SEQN_I
	FROM _cba__app_csel4_csel4dev_dataset_appt__pdct__acct__i__cse__ccc__bus__app__prod__20060101
)

SELECT * FROM TgtApptPdctRpayInsertDS