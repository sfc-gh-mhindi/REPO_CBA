{{ config(materialized='view', tags=['LdAcctApptPdct_Upd']) }}

WITH 
_cba__app_csel4_dev_dataset_acct__appt__pdct__u__cse__chl__bus__hlm__app__20100614 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_dataset_acct__appt__pdct__u__cse__chl__bus__hlm__app__20100614")  }})
TgtAcctApptUpdateDS AS (
	SELECT APPT_PDCT_I,
		ACCT_I,
		EFFT_D,
		EXPY_D,
		PROS_KEY_EXPY_I
	FROM _cba__app_csel4_dev_dataset_acct__appt__pdct__u__cse__chl__bus__hlm__app__20100614
)

SELECT * FROM TgtAcctApptUpdateDS