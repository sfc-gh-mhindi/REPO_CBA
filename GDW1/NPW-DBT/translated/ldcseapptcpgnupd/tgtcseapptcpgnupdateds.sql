{{ config(materialized='view', tags=['LdCseApptCpgnUpd']) }}

WITH 
_cba__app_csel4_dev_dataset_cse__appt__cpgn__u__cse__chl__bus__app__20060616 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_dataset_cse__appt__cpgn__u__cse__chl__bus__app__20060616")  }})
TgtCseApptCpgnUpdateDS AS (
	SELECT APPT_I,
		EFFT_D,
		EXPY_D,
		PROS_KEY_EXPY_I
	FROM _cba__app_csel4_dev_dataset_cse__appt__cpgn__u__cse__chl__bus__app__20060616
)

SELECT * FROM TgtCseApptCpgnUpdateDS