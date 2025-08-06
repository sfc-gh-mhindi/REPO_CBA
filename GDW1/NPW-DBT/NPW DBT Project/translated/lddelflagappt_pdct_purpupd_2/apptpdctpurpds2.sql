{{ config(materialized='view', tags=['LdDelFlagAPPT_PDCT_PURPUpd_2']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_hl__pl__app__prod__purp__appt__pdct__purp AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_hl__pl__app__prod__purp__appt__pdct__purp")  }})
ApptPdctPurpDS2 AS (
	SELECT APPT_PDCT_I,
		SRCE_SYST_APPT_PDCT_PURP_I,
		EXPY_D,
		PROS_KEY_EXPY_I
	FROM _cba__app_csel4_csel4dev_dataset_hl__pl__app__prod__purp__appt__pdct__purp
)

SELECT * FROM ApptPdctPurpDS2