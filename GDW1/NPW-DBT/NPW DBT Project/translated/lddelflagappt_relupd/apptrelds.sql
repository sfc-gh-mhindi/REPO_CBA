{{ config(materialized='view', tags=['LdDelFlagAPPT_RELUpd']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_ccl__hl__app__appt__rel AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_ccl__hl__app__appt__rel")  }})
ApptRelDS AS (
	SELECT APPT_I,
		RELD_APPT_I,
		REL_TYPE_C,
		EXPY_D,
		PROS_KEY_EXPY_I
	FROM _cba__app_csel4_csel4dev_dataset_ccl__hl__app__appt__rel
)

SELECT * FROM ApptRelDS