{{ config(materialized='view', tags=['LdApptAsetUpd']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_appt__u__cse__cpl__bus__fee__margin__20060101 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_appt__u__cse__cpl__bus__fee__margin__20060101")  }})
TgtApptAsetUpdateDS AS (
	SELECT APPT_I,
		ASET_I,
		EFFT_D,
		EXPY_D,
		pros_key_expy_i
	FROM _cba__app_csel4_csel4dev_dataset_appt__u__cse__cpl__bus__fee__margin__20060101
)

SELECT * FROM TgtApptAsetUpdateDS