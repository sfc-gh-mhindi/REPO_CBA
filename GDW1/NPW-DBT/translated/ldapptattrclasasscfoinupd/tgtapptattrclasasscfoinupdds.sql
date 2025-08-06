{{ config(materialized='view', tags=['LdApptAttrClasAsscFoinUpd']) }}

WITH 
_cba__app_csel4_dev_dataset_appt__attr__clas__assc__u__cse__chl__bus__app__20140424 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_dataset_appt__attr__clas__assc__u__cse__chl__bus__app__20140424")  }})
TgtApptAttrClasAsscFoinUpdDS AS (
	SELECT APPT_I,
		APPT_ATTR_CLAS_C,
		SRCE_SYST_C,
		EFFT_D,
		EXPY_D,
		PROS_KEY_EXPY_I
	FROM _cba__app_csel4_dev_dataset_appt__attr__clas__assc__u__cse__chl__bus__app__20140424
)

SELECT * FROM TgtApptAttrClasAsscFoinUpdDS