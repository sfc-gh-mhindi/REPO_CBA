{{ config(materialized='view', tags=['LdApptGnrcDate_Ins']) }}

WITH 
_cba__app_csel4_dev_dataset_appt__pdct__unid__paty__i__cse__chl__bus__hlm__app__20100614 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_dataset_appt__pdct__unid__paty__i__cse__chl__bus__hlm__app__20100614")  }})
TgtApptgnrcDtInsertDS AS (
	SELECT APPT_I,
		DATE_ROLE_C,
		EFFT_D,
		GNRC_ROLE_S,
		GNRC_ROLE_D,
		GNRC_ROLE_T,
		EXPY_D,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		EROR_SEQN_I,
		MODF_S,
		MODF_D,
		MODF_T,
		USER_I,
		CHNG_REAS_TYPE_C
	FROM _cba__app_csel4_dev_dataset_appt__pdct__unid__paty__i__cse__chl__bus__hlm__app__20100614
)

SELECT * FROM TgtApptgnrcDtInsertDS