{{ config(materialized='view', tags=['LdApptPdctPatyIns']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_appt__pdct__paty__i__cse__cpl__bus__fee__margin__20060101 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_appt__pdct__paty__i__cse__cpl__bus__fee__margin__20060101")  }})
TgtApptPdctPatyInsertDS AS (
	SELECT APPT_PDCT_I,
		PATY_I,
		PATY_ROLE_C,
		EFFT_D,
		SRCE_SYST_C,
		SRCE_SYST_APPT_PDCT_PATY_I,
		EXPY_D,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		EROR_SEQN_I
	FROM _cba__app_csel4_csel4dev_dataset_appt__pdct__paty__i__cse__cpl__bus__fee__margin__20060101
)

SELECT * FROM TgtApptPdctPatyInsertDS