{{ config(materialized='view', tags=['LdDelFlagAPPT_PDCT_ACCTUpd_2']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_hl__pl__cc__app__prod__appt__pdct__acct AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_hl__pl__cc__app__prod__appt__pdct__acct")  }})
ApptPdctAcctDS2 AS (
	SELECT APPT_PDCT_I,
		EXPY_D,
		PROS_KEY_EXPY_I
	FROM _cba__app_csel4_csel4dev_dataset_hl__pl__cc__app__prod__appt__pdct__acct
)

SELECT * FROM ApptPdctAcctDS2