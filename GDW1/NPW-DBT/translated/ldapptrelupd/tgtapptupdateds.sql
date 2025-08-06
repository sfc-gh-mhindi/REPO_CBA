{{ config(materialized='view', tags=['LdApptRelUpd']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_appt__rel__u__cse__ccc__bus__app__prod__20060101 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_appt__rel__u__cse__ccc__bus__app__prod__20060101")  }})
TgtApptUpdateDS AS (
	SELECT APPT_I,
		RELD_APPT_I,
		REL_TYPE_C,
		EFFT_D,
		EXPY_D,
		PROS_KEY_EXPY_I
	FROM _cba__app_csel4_csel4dev_dataset_appt__rel__u__cse__ccc__bus__app__prod__20060101
)

SELECT * FROM TgtApptUpdateDS