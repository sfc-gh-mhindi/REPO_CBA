{{ config(materialized='view', tags=['LdApptPdctPatyUpd']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_appt__pdct__paty__u__cse__cpl__bus__fee__margin__20060101 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_appt__pdct__paty__u__cse__cpl__bus__fee__margin__20060101")  }})
TgtApptPdctPatyUpdateDS AS (
	SELECT APPT_PDCT_I,
		PATY_I,
		PATY_ROLE_C,
		EFFT_D,
		PROS_KEY_EXPY_I,
		EXPY_D
	FROM _cba__app_csel4_csel4dev_dataset_appt__pdct__paty__u__cse__cpl__bus__fee__margin__20060101
)

SELECT * FROM TgtApptPdctPatyUpdateDS