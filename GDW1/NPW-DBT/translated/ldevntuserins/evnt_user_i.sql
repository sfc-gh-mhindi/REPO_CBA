{{ config(materialized='view', tags=['LdEvntUserIns']) }}

WITH 
_cba__app01_csel4_dev_dataset_evnt__user__i__cse__onln__bus__clnt__rm__rate__20100303 AS (
	SELECT
	*
	FROM {{ source("","_cba__app01_csel4_dev_dataset_evnt__user__i__cse__onln__bus__clnt__rm__rate__20100303")  }})
EVNT_USER_I AS (
	SELECT EVNT_I,
		USER_I,
		EVNT_PATY_ROLE_C,
		EFFT_D,
		EXPY_D,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		EROR_SEQN_I
	FROM _cba__app01_csel4_dev_dataset_evnt__user__i__cse__onln__bus__clnt__rm__rate__20100303
)

SELECT * FROM EVNT_USER_I