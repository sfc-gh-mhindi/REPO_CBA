{{ config(materialized='view', tags=['LdApptRelUpd_3']) }}

WITH 
_cba__app_lpxs_lpxs__dev_dataset_appt__rel__3__u__cse__com__bus__app__prod__ccl__pl__app__prod__20071220 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_lpxs_lpxs__dev_dataset_appt__rel__3__u__cse__com__bus__app__prod__ccl__pl__app__prod__20071220")  }})
TgtApptUpdateDS AS (
	SELECT APPT_I,
		REL_TYPE_C,
		EFFT_D,
		EXPY_D,
		PROS_KEY_EXPY_I
	FROM _cba__app_lpxs_lpxs__dev_dataset_appt__rel__3__u__cse__com__bus__app__prod__ccl__pl__app__prod__20071220
)

SELECT * FROM TgtApptUpdateDS