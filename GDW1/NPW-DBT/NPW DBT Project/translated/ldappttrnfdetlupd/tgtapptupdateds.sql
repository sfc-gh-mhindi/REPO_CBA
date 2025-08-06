{{ config(materialized='view', tags=['LdApptTrnfDetlUpd']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_appt__trnf__detl__u__cse__ccc__bus__app__prod__bal__xfer__20060101 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_appt__trnf__detl__u__cse__ccc__bus__app__prod__bal__xfer__20060101")  }})
TgtApptUpdateDS AS (
	SELECT APPT_I,
		APPT_TRNF_I,
		EFFT_D,
		EXPY_D,
		PROS_KEY_EXPY_I
	FROM _cba__app_csel4_csel4dev_dataset_appt__trnf__detl__u__cse__ccc__bus__app__prod__bal__xfer__20060101
)

SELECT * FROM TgtApptUpdateDS