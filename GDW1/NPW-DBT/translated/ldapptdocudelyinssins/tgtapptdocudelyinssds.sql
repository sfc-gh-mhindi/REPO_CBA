{{ config(materialized='view', tags=['LdApptDocuDelyInssIns']) }}

WITH 
_cba__app_csel4_dev_dataset_appt__docu__dely__inss__i__cse__chl__bus__app__20100914 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_dataset_appt__docu__dely__inss__i__cse__chl__bus__app__20100914")  }})
TgtApptDocuDelyInssDS AS (
	SELECT APPT_I,
		SRCE_SYST_C,
		DOCU_DELY_RECV_C,
		EFFT_D,
		EXPY_D,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I
	FROM _cba__app_csel4_dev_dataset_appt__docu__dely__inss__i__cse__chl__bus__app__20100914
)

SELECT * FROM TgtApptDocuDelyInssDS