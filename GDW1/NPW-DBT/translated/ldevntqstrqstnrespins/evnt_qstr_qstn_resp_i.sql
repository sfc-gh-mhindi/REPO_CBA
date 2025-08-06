{{ config(materialized='view', tags=['LdEvntQstrQstnRespIns']) }}

WITH 
_cba__app01_csel4_dev_dataset_evnt__qstr__qstn__resp__i__cse__onln__bus__clnt__rm__rate__20100303 AS (
	SELECT
	*
	FROM {{ source("","_cba__app01_csel4_dev_dataset_evnt__qstr__qstn__resp__i__cse__onln__bus__clnt__rm__rate__20100303")  }})
EVNT_QSTR_QSTN_RESP_I AS (
	SELECT EVNT_I,
		QSTR_C,
		QSTN_C,
		RESP_C,
		RESP_VALU_N,
		RESP_VALU_S,
		RESP_VALU_D,
		RESP_VALU_T,
		RESP_VALU_X,
		EFFT_D,
		EXPY_D,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		ROW_SECU_ACCS_C
	FROM _cba__app01_csel4_dev_dataset_evnt__qstr__qstn__resp__i__cse__onln__bus__clnt__rm__rate__20100303
)

SELECT * FROM EVNT_QSTR_QSTN_RESP_I