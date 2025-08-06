{{ config(materialized='view', tags=['LdApptRelIns_3']) }}

WITH 
_cba__app_lpxs_lpxs__dev_dataset_appt__rel__3__i__cse__com__bus__app__prod__ccl__pl__app__prod__20071220 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_lpxs_lpxs__dev_dataset_appt__rel__3__i__cse__com__bus__app__prod__ccl__pl__app__prod__20071220")  }})
TgtApptRelInsertDS AS (
	SELECT APPT_I,
		RELD_APPT_I,
		REL_TYPE_C,
		EFFT_D,
		EXPY_D,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		EROR_SEQN_I
	FROM _cba__app_lpxs_lpxs__dev_dataset_appt__rel__3__i__cse__com__bus__app__prod__ccl__pl__app__prod__20071220
)

SELECT * FROM TgtApptRelInsertDS