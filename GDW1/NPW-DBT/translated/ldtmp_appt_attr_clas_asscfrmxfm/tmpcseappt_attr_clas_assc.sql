{{ config(materialized='view', tags=['LdTMP_APPT_ATTR_CLAS_ASSCFrmXfm']) }}

WITH 
_cba__app_csel4_sit1_dataset_tmp__cse__chl__bus__app__cse__appt__attr__clas__assc AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_sit1_dataset_tmp__cse__chl__bus__app__cse__appt__attr__clas__assc")  }})
TmpCseAppt_Attr_Clas_Assc AS (
	SELECT APPT_I,
		APPT_ATTR_CLAS_C,
		APPT_ATTR_CLAS_VALU_C,
		EFFT_D,
		SRCE_SYST_C,
		EXPY_D,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		ROW_SECU_ACCS_C
	FROM _cba__app_csel4_sit1_dataset_tmp__cse__chl__bus__app__cse__appt__attr__clas__assc
)

SELECT * FROM TmpCseAppt_Attr_Clas_Assc