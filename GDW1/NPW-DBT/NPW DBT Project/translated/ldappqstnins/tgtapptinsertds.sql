{{ config(materialized='view', tags=['LdAppQstnIns']) }}

WITH 
_cba__app_csel4_csel4__prd_dataset_appt__qstn__i__cse__clp__bus__appt__qstn__20080402 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4__prd_dataset_appt__qstn__i__cse__clp__bus__appt__qstn__20080402")  }})
TgtApptInsertDS AS (
	SELECT APPT_I,
		QSTN_C,
		EFFT_D,
		RESP_C,
		RESP_CMMT_X,
		EXPY_D,
		PATY_I,
		ROW_SECU_ACCS_C,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		EROR_SEQN_I
	FROM _cba__app_csel4_csel4__prd_dataset_appt__qstn__i__cse__clp__bus__appt__qstn__20080402
)

SELECT * FROM TgtApptInsertDS