{{ config(materialized='view', tags=['LdEvntAntnIns']) }}

WITH 
_cba__app01_csel4_dev_dataset_evnt__antn__i__cse__onln__bus__clnt__rm__rate__20100303 AS (
	SELECT
	*
	FROM {{ source("","_cba__app01_csel4_dev_dataset_evnt__antn__i__cse__onln__bus__clnt__rm__rate__20100303")  }})
EVNT_ANTN_I AS (
	SELECT EVNT_I,
		ANTN_I,
		SRCE_SYST_C,
		EFFT_D,
		EXPY_D,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		EROR_SEQN_I,
		ROW_SECU_ACCS_C
	FROM _cba__app01_csel4_dev_dataset_evnt__antn__i__cse__onln__bus__clnt__rm__rate__20100303
)

SELECT * FROM EVNT_ANTN_I