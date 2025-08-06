{{ config(materialized='view', tags=['LdDelFlagAPPT_FEATUpd']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_ccl__app__fee__appt__feat AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_ccl__app__fee__appt__feat")  }})
ApptFeatDS AS (
	SELECT APPT_I,
		SRCE_SYST_APPT_FEAT_I,
		EXPY_D,
		PROS_KEY_EXPY_I
	FROM _cba__app_csel4_csel4dev_dataset_ccl__app__fee__appt__feat
)

SELECT * FROM ApptFeatDS