{{ config(materialized='view', tags=['LdApptPdctFeat_Upd']) }}

WITH 
_cba__app_csel4_dev_dataset_appt__pdct__feat__u__cse__chl__bus__hlm__app__20150120 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_dataset_appt__pdct__feat__u__cse__chl__bus__hlm__app__20150120")  }})
TgtAppPdctFeatUpdateDS AS (
	SELECT APPT_PDCT_I,
		FEAT_I,
		SRCE_SYST_C,
		EFFT_D,
		EXPY_D,
		PROS_KEY_EXPY_I
	FROM _cba__app_csel4_dev_dataset_appt__pdct__feat__u__cse__chl__bus__hlm__app__20150120
)

SELECT * FROM TgtAppPdctFeatUpdateDS