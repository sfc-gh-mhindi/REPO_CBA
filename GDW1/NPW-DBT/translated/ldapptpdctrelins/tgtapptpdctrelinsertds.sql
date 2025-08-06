{{ config(materialized='view', tags=['LdApptPdctRelIns']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_appt__pdct__rel__i__cse__gdw__appt__pdct__20060101 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_appt__pdct__rel__i__cse__gdw__appt__pdct__20060101")  }})
TgtApptPdctRelInsertDS AS (
	SELECT APPT_PDCT_I,
		RELD_APPT_PDCT_I,
		EFFT_D,
		REL_TYPE_C,
		EXPY_D,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		EROR_SEQN_I
	FROM _cba__app_csel4_csel4dev_dataset_appt__pdct__rel__i__cse__gdw__appt__pdct__20060101
)

SELECT * FROM TgtApptPdctRelInsertDS