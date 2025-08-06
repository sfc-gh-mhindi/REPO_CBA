{{ config(materialized='view', tags=['LdDelFlagAPPT_PDCT_FEATUpd_4']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_join__part2__appt__pdct__feat AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_join__part2__appt__pdct__feat")  }})
JoinPart2_ApptPdctFeatDS AS (
	SELECT APPT_PDCT_I,
		SRCE_SYST_APPT_FEAT_I,
		EXPY_D,
		PROS_KEY_EXPY_I
	FROM _cba__app_csel4_csel4dev_dataset_join__part2__appt__pdct__feat
)

SELECT * FROM JoinPart2_ApptPdctFeatDS