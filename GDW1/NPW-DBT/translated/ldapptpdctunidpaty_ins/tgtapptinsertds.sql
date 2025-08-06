{{ config(materialized='view', tags=['LdApptPdctUnidPaty_Ins']) }}

WITH 
_cba__app_csel4_dev_dataset_appt__pdct__unid__paty__i__cse__chl__bus__hlm__app__20100614 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_dataset_appt__pdct__unid__paty__i__cse__chl__bus__hlm__app__20100614")  }})
TgtApptInsertDS AS (
	SELECT APPT_PDCT_I,
		PATY_ROLE_C,
		SRCE_SYST_PATY_I,
		EFFT_D,
		SRCE_SYST_C,
		UNID_PATY_CATG_C,
		PATY_M,
		EXPY_D,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		EROR_SEQN_I
	FROM _cba__app_csel4_dev_dataset_appt__pdct__unid__paty__i__cse__chl__bus__hlm__app__20100614
)

SELECT * FROM TgtApptInsertDS