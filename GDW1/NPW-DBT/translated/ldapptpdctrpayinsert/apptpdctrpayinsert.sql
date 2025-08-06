{{ config(materialized='view', tags=['LdApptPdctRpayInsert']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_appt__pdct__paty__i__cse__cpl__bus__fee__margin__20060101 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_appt__pdct__paty__i__cse__cpl__bus__fee__margin__20060101")  }})
ApptPdctRpayInsert AS (
	SELECT APPT_PDCT_I,
		RPAY_TYPE_C,
		EFFT_D,
		PAYT_FREQ_C,
		STRT_RPAY_D,
		EXPY_D,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		EROR_SEQN_I,
		SRCE_SYST_C,
		RPAY_SRCE_C,
		RPAY_SRCE_OTHR_X
	FROM _cba__app_csel4_csel4dev_dataset_appt__pdct__paty__i__cse__cpl__bus__fee__margin__20060101
)

SELECT * FROM ApptPdctRpayInsert