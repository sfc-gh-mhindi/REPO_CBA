{{ config(materialized='view', tags=['LdDelFlagAPPT_PDCT_FEATUpd_2']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_cc__appt__pdct__feat AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_cc__appt__pdct__feat")  }})
CC_ApptPdctFeatDS AS (
	SELECT APPT_PDCT_I,
		EXPY_D,
		PROS_KEY_EXPY_I
	FROM _cba__app_csel4_csel4dev_dataset_cc__appt__pdct__feat
)

SELECT * FROM CC_ApptPdctFeatDS