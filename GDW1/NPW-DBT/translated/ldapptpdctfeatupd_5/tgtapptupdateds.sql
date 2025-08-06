{{ config(materialized='view', tags=['LdApptPdctFeatUpd_5']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_appt__pdct__feat__5__u__cse__cpl__bus__fee__margin__20060101 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_appt__pdct__feat__5__u__cse__cpl__bus__fee__margin__20060101")  }})
TgtApptUpdateDS AS (
	SELECT APPT_PDCT_I,
		FEAT_I,
		EFFT_D,
		EXPY_D,
		PROS_KEY_EXPY_I
	FROM _cba__app_csel4_csel4dev_dataset_appt__pdct__feat__5__u__cse__cpl__bus__fee__margin__20060101
)

SELECT * FROM TgtApptUpdateDS