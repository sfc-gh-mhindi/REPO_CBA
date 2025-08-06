{{ config(materialized='view', tags=['LdDelFlagACCT_APPT_PDCTUpd_1']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_ccl__app__prod__acct__appt__pdct AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_ccl__app__prod__acct__appt__pdct")  }})
AcctApptPdctDS1 AS (
	SELECT ACCT_I,
		APPT_PDCT_I,
		EXPY_D,
		PROS_KEY_EXPY_I
	FROM _cba__app_csel4_csel4dev_dataset_ccl__app__prod__acct__appt__pdct
)

SELECT * FROM AcctApptPdctDS1