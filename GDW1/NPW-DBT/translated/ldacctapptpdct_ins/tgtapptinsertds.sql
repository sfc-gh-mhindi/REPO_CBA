{{ config(materialized='view', tags=['LdAcctApptPdct_Ins']) }}

WITH 
_cba__app_csel4_dev_dataset_acct__appt__pdct__i__cse__chl__bus__hlm__app__20100614 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_dataset_acct__appt__pdct__i__cse__chl__bus__hlm__app__20100614")  }})
TgtApptInsertDS AS (
	SELECT APPT_PDCT_I,
		ACCT_I,
		REL_TYPE_C,
		EFFT_D,
		EXPY_D,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		EROR_SEQN_I
	FROM _cba__app_csel4_dev_dataset_acct__appt__pdct__i__cse__chl__bus__hlm__app__20100614
)

SELECT * FROM TgtApptInsertDS