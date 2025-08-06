{{ config(materialized='view', tags=['LdDelFlagAPPT_PDCT_PATYUpd_2']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_ccl__app__prod__appt__pdct__paty AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_ccl__app__prod__appt__pdct__paty")  }})
SrcApptPdctPatyDS2 AS (
	SELECT APPT_PDCT_I,
		PATY_ROLE_C,
		EXPY_D,
		PROS_KEY_EXPY_I
	FROM _cba__app_csel4_csel4dev_dataset_ccl__app__prod__appt__pdct__paty
)

SELECT * FROM SrcApptPdctPatyDS2