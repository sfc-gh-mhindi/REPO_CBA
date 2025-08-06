{{ config(materialized='view', tags=['LdApptEvntGrupUpd']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_appt__pdct__acct__u__cse__ccc__bus__app__prod__20060101 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_appt__pdct__acct__u__cse__ccc__bus__app__prod__20060101")  }})
TgtApptEvntGrupUpdateDS AS (
	SELECT APPT_I,
		EFFT_D,
		EXPY_D,
		PROS_KEY_EXPY_I
	FROM _cba__app_csel4_csel4dev_dataset_appt__pdct__acct__u__cse__ccc__bus__app__prod__20060101
)

SELECT * FROM TgtApptEvntGrupUpdateDS