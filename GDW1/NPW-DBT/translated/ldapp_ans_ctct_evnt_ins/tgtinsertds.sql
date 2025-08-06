{{ config(materialized='view', tags=['LdAPP_ANS_CTCT_EVNT_Ins']) }}

WITH 
_cba__app_lpxs_lpxs__dev_dataset_ctct__evnt__i__cse__xs__chl__cpl__bus__app__ans__20080214 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_lpxs_lpxs__dev_dataset_ctct__evnt__i__cse__xs__chl__cpl__bus__app__ans__20080214")  }})
TgtInsertDS AS (
	SELECT EVNT_I,
		SRCE_SYST_EVNT_I,
		EVNT_ACTL_D,
		SRCE_SYST_C,
		PROS_KEY_EFFT_I,
		EROR_SEQN_I,
		SRCE_SYST_EVNT_TYPE_I,
		CTCT_EVNT_TYPE_C,
		EVNT_ACTL_T,
		ROW_SECU_ACCS_C
	FROM _cba__app_lpxs_lpxs__dev_dataset_ctct__evnt__i__cse__xs__chl__cpl__bus__app__ans__20080214
)

SELECT * FROM TgtInsertDS