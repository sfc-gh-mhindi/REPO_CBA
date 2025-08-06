{{ config(materialized='view', tags=['LdBusnEvnt1Ins']) }}

WITH 
_cba__app01_csel4_dev_dataset_busn__evnt__i__cse__onln__bus__clnt__rm__rate__20100303 AS (
	SELECT
	*
	FROM {{ source("","_cba__app01_csel4_dev_dataset_busn__evnt__i__cse__onln__bus__clnt__rm__rate__20100303")  }})
BUSN_EVNT_I AS (
	SELECT EVNT_I,
		SRCE_SYST_EVNT_I,
		EVNT_ACTL_D,
		SRCE_SYST_C,
		PROS_KEY_EFFT_I,
		EROR_SEQN_I,
		SRCE_SYST_EVNT_TYPE_I,
		EVNT_ACTL_T,
		ROW_SECU_ACCS_C
	FROM _cba__app01_csel4_dev_dataset_busn__evnt__i__cse__onln__bus__clnt__rm__rate__20100303
)

SELECT * FROM BUSN_EVNT_I