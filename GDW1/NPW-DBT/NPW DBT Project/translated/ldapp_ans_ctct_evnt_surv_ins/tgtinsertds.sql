{{ config(materialized='view', tags=['LdAPP_ANS_CTCT_EVNT_SURV_Ins']) }}

WITH 
_cba__app_lpxs_lpxs__dev_dataset_ctct__evnt__surv__i__lpxs__app__ans__20071119 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_lpxs_lpxs__dev_dataset_ctct__evnt__surv__i__lpxs__app__ans__20071119")  }})
TgtInsertDS AS (
	SELECT EVNT_I,
		QSTN_C,
		EFFT_D,
		RESP_C,
		RESP_CMMT_X,
		EXPY_D,
		ROW_SECU_ACCS_C,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		EROR_SEQN_I
	FROM _cba__app_lpxs_lpxs__dev_dataset_ctct__evnt__surv__i__lpxs__app__ans__20071119
)

SELECT * FROM TgtInsertDS