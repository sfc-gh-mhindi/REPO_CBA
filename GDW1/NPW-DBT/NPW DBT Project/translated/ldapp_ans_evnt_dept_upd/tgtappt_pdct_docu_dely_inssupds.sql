{{ config(materialized='view', tags=['LdAPP_ANS_EVNT_DEPT_Upd']) }}

WITH 
_cba__app_lpxs_lpxs__dev_dataset_evnt__dept__u__chl__bus__hl__app__ans__20071122 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_lpxs_lpxs__dev_dataset_evnt__dept__u__chl__bus__hl__app__ans__20071122")  }})
TgtAPPT_PDCT_DOCU_DELY_INSSUpDS AS (
	SELECT EVNT_I,
		DEPT_ROLE_C,
		EXPY_D,
		PROS_KEY_EXPY_I
	FROM _cba__app_lpxs_lpxs__dev_dataset_evnt__dept__u__chl__bus__hl__app__ans__20071122
)

SELECT * FROM TgtAPPT_PDCT_DOCU_DELY_INSSUpDS