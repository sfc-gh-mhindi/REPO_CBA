{{ config(materialized='view', tags=['LdAPP_ANS_EVNT_EMPL_Upd']) }}

WITH 
_cba__app_lpxs_lpxs__dev_dataset_evnt__empl__u__lpxs__app__ans__20071119 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_lpxs_lpxs__dev_dataset_evnt__empl__u__lpxs__app__ans__20071119")  }})
TgtAPPT_PDCT_DOCU_DELY_INSSUpDS AS (
	SELECT EVNT_I,
		EVNT_PATY_ROLE_TYPE_C,
		EXPY_D,
		PROS_KEY_EXPY_I
	FROM _cba__app_lpxs_lpxs__dev_dataset_evnt__empl__u__lpxs__app__ans__20071119
)

SELECT * FROM TgtAPPT_PDCT_DOCU_DELY_INSSUpDS