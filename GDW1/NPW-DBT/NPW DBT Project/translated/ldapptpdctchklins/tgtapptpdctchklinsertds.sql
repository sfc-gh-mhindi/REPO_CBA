{{ config(materialized='view', tags=['LdApptPdctChklIns']) }}

WITH 
_cba__app_csel4_dev_dataset_appt__pdct__chkl__i__cse__cpl__bus__app__prod__20101211 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_dataset_appt__pdct__chkl__i__cse__cpl__bus__app__prod__20101211")  }})
TgtApptPdctChklInsertDS AS (
	SELECT APPT_PDCT_I,
		CHKL_ITEM_C,
		STUS_D,
		STUS_C,
		SRCE_SYST_C,
		CHKL_ITEM_X,
		EFFT_D,
		EXPY_D,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I
	FROM _cba__app_csel4_dev_dataset_appt__pdct__chkl__i__cse__cpl__bus__app__prod__20101211
)

SELECT * FROM TgtApptPdctChklInsertDS