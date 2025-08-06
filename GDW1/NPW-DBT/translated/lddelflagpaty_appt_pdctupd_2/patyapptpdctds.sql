{{ config(materialized='view', tags=['LdDelFlagPATY_APPT_PDCTUpd_2']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_ccl__app__prod__paty__appt__pdct AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_ccl__app__prod__paty__appt__pdct")  }})
PatyApptPdctDS AS (
	SELECT PATY_I,
		APPT_PDCT_I,
		PATY_ROLE_C,
		EXPY_D,
		PROS_KEY_EXPY_I
	FROM _cba__app_csel4_csel4dev_dataset_ccl__app__prod__paty__appt__pdct
)

SELECT * FROM PatyApptPdctDS