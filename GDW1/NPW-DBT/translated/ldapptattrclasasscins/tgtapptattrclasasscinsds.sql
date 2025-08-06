{{ config(materialized='view', tags=['LdApptAttrClasAsscIns']) }}

WITH 
_cba__app_csel4_dev_dataset_appt__attr__clas__assc__i__cse__chl__bus__app__20140424 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_dataset_appt__attr__clas__assc__i__cse__chl__bus__app__20140424")  }})
TgtApptAttrClasAsscInsDS AS (
	SELECT APPT_I,
		APPT_ATTR_CLAS_C,
		APPT_ATTR_CLAS_VALU_C,
		EFFT_D,
		SRCE_SYST_C,
		EXPY_D,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		ROW_SECU_ACCS_C
	FROM _cba__app_csel4_dev_dataset_appt__attr__clas__assc__i__cse__chl__bus__app__20140424
)

SELECT * FROM TgtApptAttrClasAsscInsDS