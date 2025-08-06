{{ config(materialized='view', tags=['LdApptPdctChklUpd']) }}

WITH 
_cba__app_csel4_dev_dataset_appt__pdct__chkl__u__cse__cpl__bus__app__prod__20101211 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_dataset_appt__pdct__chkl__u__cse__cpl__bus__app__prod__20101211")  }})
TgtApptPdctChklUpdateDS AS (
	SELECT APPT_PDCT_I,
		CHKL_ITEM_C,
		SRCE_SYST_C,
		EFFT_D,
		EXPY_D,
		PROS_KEY_EXPY_I
	FROM _cba__app_csel4_dev_dataset_appt__pdct__chkl__u__cse__cpl__bus__app__prod__20101211
)

SELECT * FROM TgtApptPdctChklUpdateDS