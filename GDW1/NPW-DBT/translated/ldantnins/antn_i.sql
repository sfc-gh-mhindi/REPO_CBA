{{ config(materialized='view', tags=['LdAntnIns']) }}

WITH 
_cba__app01_csel4_dev_dataset_antn__i__cse__onln__bus__clnt__rm__rate__20100303 AS (
	SELECT
	*
	FROM {{ source("","_cba__app01_csel4_dev_dataset_antn__i__cse__onln__bus__clnt__rm__rate__20100303")  }})
ANTN_I AS (
	SELECT ANTN_I,
		ANTN_TYPE_C,
		ANTN_X,
		SRCE_SYST_C,
		SRCE_SYST_ANTN_I,
		ANTN_S,
		ANTN_D,
		ANTN_T,
		EMPL_I,
		USER_I,
		PRVT_F,
		EFFT_D,
		EXPY_D,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		EROR_SEQN_I
	FROM _cba__app01_csel4_dev_dataset_antn__i__cse__onln__bus__clnt__rm__rate__20100303
)

SELECT * FROM ANTN_I