{{ config(materialized='view', tags=['LdApptEmplIns']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_appt__empl__i__cse__com__bus__app__20060616 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_appt__empl__i__cse__com__bus__app__20060616")  }})
TgtApptEmplInsertDS AS (
	SELECT APPT_I,
		EMPL_I,
		EMPL_ROLE_C,
		EFFT_D,
		EXPY_D,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		EROR_SEQN_I
	FROM _cba__app_csel4_csel4dev_dataset_appt__empl__i__cse__com__bus__app__20060616
)

SELECT * FROM TgtApptEmplInsertDS