{{ config(materialized='view', tags=['LdDelFlagAPPT_PDCT_RELUpd_1']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_ccl__app__prod__appt__pdct__rel AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_ccl__app__prod__appt__pdct__rel")  }})
ApptPdctRelDS1 AS (
	SELECT APPT_PDCT_I,
		RELD_APPT_PDCT_I,
		EXPY_D,
		PROS_KEY_EXPY_I
	FROM _cba__app_csel4_csel4dev_dataset_ccl__app__prod__appt__pdct__rel
)

SELECT * FROM ApptPdctRelDS1