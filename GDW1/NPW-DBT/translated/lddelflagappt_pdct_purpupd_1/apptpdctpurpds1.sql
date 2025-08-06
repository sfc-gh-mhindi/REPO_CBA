{{ config(materialized='view', tags=['LdDelFlagAPPT_PDCT_PURPUpd_1']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_ccl__app__prod__appt__pdct__purp AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_ccl__app__prod__appt__pdct__purp")  }})
ApptPdctPurpDS1 AS (
	SELECT APPT_PDCT_I,
		EXPY_D,
		PROS_KEY_EXPY_I
	FROM _cba__app_csel4_csel4dev_dataset_ccl__app__prod__appt__pdct__purp
)

SELECT * FROM ApptPdctPurpDS1