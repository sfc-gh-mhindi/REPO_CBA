{{ config(materialized='view', tags=['LdDelFlagAPPT_PDCT_ACCTUpd_1']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_ccl__app__prod__appt__pdct__acct AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_ccl__app__prod__appt__pdct__acct")  }})
ApptPdctAcctDS1 AS (
	SELECT APPT_PDCT_I,
		EXPY_D,
		PROS_KEY_EXPY_I
	FROM _cba__app_csel4_csel4dev_dataset_ccl__app__prod__appt__pdct__acct
)

SELECT * FROM ApptPdctAcctDS1