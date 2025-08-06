{{ config(materialized='view', tags=['LdDelFlagAPPT_PDCT_FEATUpd_1']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_cl__hl__pl__appt__pdct__feat AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_cl__hl__pl__appt__pdct__feat")  }})
CL_HL_PL_OutApptPdctFeatDS AS (
	SELECT APPT_PDCT_I,
		SRCE_SYST_APPT_FEAT_I,
		EXPY_D,
		PROS_KEY_EXPY_I
	FROM _cba__app_csel4_csel4dev_dataset_cl__hl__pl__appt__pdct__feat
)

SELECT * FROM CL_HL_PL_OutApptPdctFeatDS