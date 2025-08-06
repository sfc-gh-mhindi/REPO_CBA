{{ config(materialized='view', tags=['LdApptRelIns_2']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_appt__rel__2__i__cse__ccc__bus__app__prod__20060101 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_appt__rel__2__i__cse__ccc__bus__app__prod__20060101")  }})
TgtApptRelInsertDS AS (
	SELECT APPT_I,
		RELD_APPT_I,
		REL_TYPE_C,
		EFFT_D,
		EXPY_D,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		EROR_SEQN_I
	FROM _cba__app_csel4_csel4dev_dataset_appt__rel__2__i__cse__ccc__bus__app__prod__20060101
)

SELECT * FROM TgtApptRelInsertDS