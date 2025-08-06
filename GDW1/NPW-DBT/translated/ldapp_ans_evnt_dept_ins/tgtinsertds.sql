{{ config(materialized='view', tags=['LdAPP_ANS_EVNT_DEPT_Ins']) }}

WITH 
_cba__app_lpxs_lpxs__dev_dataset_evnt__dept__i__cse__xs__chl__cpl__bus__app__ans__20080124 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_lpxs_lpxs__dev_dataset_evnt__dept__i__cse__xs__chl__cpl__bus__app__ans__20080124")  }})
TgtInsertDS AS (
	SELECT EVNT_I,
		DEPT_ROLE_C,
		EFFT_D,
		DEPT_I,
		DEPT_RPRT_I,
		TEAM_I,
		EXPY_D,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		EROR_SEQN_I,
		ROW_SECU_ACCS_C
	FROM _cba__app_lpxs_lpxs__dev_dataset_evnt__dept__i__cse__xs__chl__cpl__bus__app__ans__20080124
)

SELECT * FROM TgtInsertDS