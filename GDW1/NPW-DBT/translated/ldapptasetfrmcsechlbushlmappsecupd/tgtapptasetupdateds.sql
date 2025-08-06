{{ config(materialized='view', tags=['LdApptAsetFrmCseChlBusHlmAppSecUpd']) }}

WITH 
_cba__app_csel4_dev_dataset_appt__aset__u__cse__chl__bus__hlm__app__sec__20100616 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_dataset_appt__aset__u__cse__chl__bus__hlm__app__sec__20100616")  }})
TgtApptAsetUpdateDS AS (
	SELECT APPT_I,
		ASET_I,
		EFFT_D,
		EXPY_D,
		pros_key_expy_i
	FROM _cba__app_csel4_dev_dataset_appt__aset__u__cse__chl__bus__hlm__app__sec__20100616
)

SELECT * FROM TgtApptAsetUpdateDS