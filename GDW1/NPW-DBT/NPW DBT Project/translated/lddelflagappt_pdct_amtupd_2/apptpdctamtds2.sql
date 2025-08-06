{{ config(materialized='view', tags=['LdDelFlagAPPT_PDCT_AMTUpd_2']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_hl__pl__cc__app__prod__appt__pdct__amt AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_hl__pl__cc__app__prod__appt__pdct__amt")  }})
ApptPdctAmtDS2 AS (
	SELECT APPT_PDCT_I,
		EXPY_D,
		PROS_KEY_EXPY_I
	FROM _cba__app_csel4_csel4dev_dataset_hl__pl__cc__app__prod__appt__pdct__amt
)

SELECT * FROM ApptPdctAmtDS2