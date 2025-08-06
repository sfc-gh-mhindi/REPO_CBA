{{ config(materialized='view', tags=['LdApptTrnfDetlIns']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_appt__pdct__acct__i__cse__ccc__bus__app__prod__20060101 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_appt__pdct__acct__i__cse__ccc__bus__app__prod__20060101")  }})
TgtApptTrnfDetlInsertDS AS (
	SELECT APPT_I,
		APPT_TRNF_I,
		EFFT_D,
		TRNF_OPTN_C,
		TRNF_A,
		CNCY_C,
		CMPE_I,
		EXPY_D,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		EROR_SEQN_I
	FROM _cba__app_csel4_csel4dev_dataset_appt__pdct__acct__i__cse__ccc__bus__app__prod__20060101
)

SELECT * FROM TgtApptTrnfDetlInsertDS