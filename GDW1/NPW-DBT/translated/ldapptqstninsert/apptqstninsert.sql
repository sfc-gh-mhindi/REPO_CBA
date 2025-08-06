{{ config(materialized='view', tags=['LdApptQstnInsert']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_appt__pdct__paty__i__cse__cpl__bus__fee__margin__20060101 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_appt__pdct__paty__i__cse__cpl__bus__fee__margin__20060101")  }})
ApptQstnInsert AS (
	SELECT APPT_I,
		QSTN_C,
		RESP_C,
		RESP_CMMT_X,
		PATY_I,
		EFFT_D,
		EXPY_D,
		ROW_SECU_ACCS_C,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		EROR_SEQN_I
	FROM _cba__app_csel4_csel4dev_dataset_appt__pdct__paty__i__cse__cpl__bus__fee__margin__20060101
)

SELECT * FROM ApptQstnInsert