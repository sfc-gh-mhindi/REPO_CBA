{{ config(materialized='view', tags=['LdCseApptHlCondApprIns']) }}

WITH 
_cba__app_csel4_dev_dataset_appt__pdct__feat__i__cse__chl__bus__app__20150115 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_dataset_appt__pdct__feat__i__cse__chl__bus__app__20150115")  }})
TgtApptHlCondApprInsertDS AS (
	SELECT APPT_I,
		EFFT_D,
		COND_APPR_F,
		COND_APPR_CONV_TO_FULL_D,
		EXPY_D,
		ROW_SECU_ACCS_C,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I
	FROM _cba__app_csel4_dev_dataset_appt__pdct__feat__i__cse__chl__bus__app__20150115
)

SELECT * FROM TgtApptHlCondApprInsertDS