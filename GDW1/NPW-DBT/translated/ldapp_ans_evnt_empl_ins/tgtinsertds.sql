{{ config(materialized='view', tags=['LdAPP_ANS_EVNT_EMPL_Ins']) }}

WITH 
_cba__app_lpxs_lpxs__dev_dataset_evnt__empl__i__lpxs__app__ans__20071119 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_lpxs_lpxs__dev_dataset_evnt__empl__i__lpxs__app__ans__20071119")  }})
TgtInsertDS AS (
	SELECT EVNT_I,
		EMPL_I,
		EVNT_PATY_ROLE_TYPE_C,
		EFFT_D,
		EXPY_D,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		EROR_SEQN_I,
		ROW_SECU_ACCS_C
	FROM _cba__app_lpxs_lpxs__dev_dataset_evnt__empl__i__lpxs__app__ans__20071119
)

SELECT * FROM TgtInsertDS