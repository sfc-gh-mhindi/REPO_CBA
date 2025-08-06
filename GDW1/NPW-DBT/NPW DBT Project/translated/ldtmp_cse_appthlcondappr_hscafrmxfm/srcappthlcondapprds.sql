{{ config(materialized='view', tags=['LdTMP_CSE_APPTHLCONDAPPR_HSCAFrmXfm']) }}

WITH 
_cba__app_csel4_dev_dataset_tmp__cse__chl__bus__app__cse__appt__hl__cond__appr AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_dataset_tmp__cse__chl__bus__app__cse__appt__hl__cond__appr")  }})
SrcApptHlCondApprDS AS (
	SELECT APPT_I,
		EFFT_D,
		COND_APPR_F,
		COND_APPR_CONV_TO_FULL_D,
		EXPY_D,
		ROW_SECU_ACCS_C
	FROM _cba__app_csel4_dev_dataset_tmp__cse__chl__bus__app__cse__appt__hl__cond__appr
)

SELECT * FROM SrcApptHlCondApprDS