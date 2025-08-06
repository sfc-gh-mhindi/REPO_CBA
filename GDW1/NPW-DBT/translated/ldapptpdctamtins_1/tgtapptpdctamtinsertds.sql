{{ config(materialized='view', tags=['LdApptPdctAmtIns_1']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_appt__pdct__amt__1__i__cse__chl__bus__app__prod__purp__20060101 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_appt__pdct__amt__1__i__cse__chl__bus__app__prod__purp__20060101")  }})
TgtApptPdctAmtInsertDS AS (
	SELECT APPT_PDCT_I,
		AMT_TYPE_C,
		EFFT_D,
		EXPY_D,
		CNCY_C,
		APPT_PDCT_A,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		EROR_SEQN_I
	FROM _cba__app_csel4_csel4dev_dataset_appt__pdct__amt__1__i__cse__chl__bus__app__prod__purp__20060101
)

SELECT * FROM TgtApptPdctAmtInsertDS