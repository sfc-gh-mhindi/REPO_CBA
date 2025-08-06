{{ config(materialized='view', tags=['LdDelFlagPATY_APPT_PDCTUpd_1']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_app__prod__client__role__paty__appt__pdct AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_app__prod__client__role__paty__appt__pdct")  }})
PatyApptPdctDS AS (
	SELECT PATY_I,
		APPT_PDCT_I,
		PATY_ROLE_C,
		EXPY_D,
		PROS_KEY_EXPY_I
	FROM _cba__app_csel4_csel4dev_dataset_app__prod__client__role__paty__appt__pdct
)

SELECT * FROM PatyApptPdctDS