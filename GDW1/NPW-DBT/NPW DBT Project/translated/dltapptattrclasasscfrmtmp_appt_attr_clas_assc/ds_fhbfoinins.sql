{{ config(materialized='incremental', alias='_cba__app_pj__rapidresponseteam_csel4_dev_dataset_appt__attr__clas__assc__i__cse__chl__bus__app__20160518', incremental_strategy='insert_overwrite', tags=['DltAPPTAttrClasAsscFrmTMP_APPT_ATTR_CLAS_ASSC']) }}

SELECT
	APPT_I,
	APPT_ATTR_CLAS_C,
	APPT_ATTR_CLAS_VALU_C,
	EFFT_D,
	SRCE_SYST_C,
	EXPY_D,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I,
	ROW_SECU_ACCS_C 
FROM {{ ref('FnFhbFoin') }}