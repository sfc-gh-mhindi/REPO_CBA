{{ config(materialized='view', tags=['LdEvntQstrIns']) }}

WITH 
_cba__app01_csel4_dev_dataset_evnt__qstr__i__cse__onln__bus__clnt__rm__rate__20100303 AS (
	SELECT
	*
	FROM {{ source("","_cba__app01_csel4_dev_dataset_evnt__qstr__i__cse__onln__bus__clnt__rm__rate__20100303")  }})
EVNT_QSTR_I AS (
	SELECT EVNT_I,
		QSTR_C,
		EFFT_D,
		EXPY_D,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I
	FROM _cba__app01_csel4_dev_dataset_evnt__qstr__i__cse__onln__bus__clnt__rm__rate__20100303
)

SELECT * FROM EVNT_QSTR_I