{{ config(materialized='view', tags=['LdApptEvntGrupIns']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_appt__pdct__acct__i__cse__ccc__bus__app__prod__20060101 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_appt__pdct__acct__i__cse__ccc__bus__app__prod__20060101")  }})
TgtApptEvntGrupInsertDS AS (
	SELECT APPT_I,
		EVNT_GRUP_I,
		EFFT_D,
		EXPY_D,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		EROR_SEQN_I
	FROM _cba__app_csel4_csel4dev_dataset_appt__pdct__acct__i__cse__ccc__bus__app__prod__20060101
)

SELECT * FROM TgtApptEvntGrupInsertDS