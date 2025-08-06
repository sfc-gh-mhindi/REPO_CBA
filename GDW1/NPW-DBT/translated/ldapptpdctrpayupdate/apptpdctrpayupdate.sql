{{ config(materialized='view', tags=['LdApptPdctRpayUpdate']) }}

WITH 
_cba__app_csel4_dev_dataset_appt__pdct__rpay__u__cse__com__cpo__bus__ncpr__clnt__20060101 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_dataset_appt__pdct__rpay__u__cse__com__cpo__bus__ncpr__clnt__20060101")  }})
ApptPdctRpayUpdate AS (
	SELECT APPT_PDCT_I,
		SRCE_SYST_C,
		EFFT_D,
		EXPY_D,
		PROS_KEY_EXPY_I
	FROM _cba__app_csel4_dev_dataset_appt__pdct__rpay__u__cse__com__cpo__bus__ncpr__clnt__20060101
)

SELECT * FROM ApptPdctRpayUpdate