{{ config(materialized='view', tags=['LdApptDocuIns']) }}

WITH 
_cba__app_csel4_dev_dataset_appt__docu__i__cse__chl__bus__app__20100920 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_dataset_appt__docu__i__cse__chl__bus__app__20100920")  }})
Appt_Docu_DS AS (
	SELECT APPT_I,
		DOCU_C,
		SRCE_SYST_C,
		DOCU_VERS_C,
		EFFT_D,
		EXPY_D,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		ROW_SECU_ACCS_C
	FROM _cba__app_csel4_dev_dataset_appt__docu__i__cse__chl__bus__app__20100920
)

SELECT * FROM Appt_Docu_DS