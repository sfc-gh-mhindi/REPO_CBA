{{ config(materialized='view', tags=['LdCseApptCpgnIns']) }}

WITH 
_cba__app_csel4_dev_dataset_cse__appt__cpgn__i__cse__chl__bus__app__20060616 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_dataset_cse__appt__cpgn__i__cse__chl__bus__app__20060616")  }})
TgtCseApptCpgnInsertDS AS (
	SELECT APPT_I,
		CSE_CPGN_CODE_X,
		EFFT_D,
		EXPY_D,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		ROW_SECU_ACCS_C
	FROM _cba__app_csel4_dev_dataset_cse__appt__cpgn__i__cse__chl__bus__app__20060616
)

SELECT * FROM TgtCseApptCpgnInsertDS