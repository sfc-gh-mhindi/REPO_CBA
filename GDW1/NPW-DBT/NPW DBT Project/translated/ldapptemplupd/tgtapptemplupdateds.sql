{{ config(materialized='view', tags=['LdApptEmplUpd']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_appt__empl__u__cse__ccc__bus__app__prod__20060101 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_appt__empl__u__cse__ccc__bus__app__prod__20060101")  }})
TgtApptEmplUpdateDS AS (
	SELECT EMPL_I,
		APPT_I,
		EMPL_ROLE_C,
		EFFT_D,
		EXPY_D,
		PROS_KEY_EXPY_I
	FROM _cba__app_csel4_csel4dev_dataset_appt__empl__u__cse__ccc__bus__app__prod__20060101
)

SELECT * FROM TgtApptEmplUpdateDS