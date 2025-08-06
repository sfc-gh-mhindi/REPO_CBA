{{ config(materialized='incremental', alias='_cba__app_pj__rapidresponseteam_csel4_dev_dataset_appt__attr__clas__assc__u__cse__chl__bus__app__20160518', incremental_strategy='insert_overwrite', tags=['DltAPPTAttrClasAsscFrmTMP_APPT_ATTR_CLAS_ASSC']) }}

SELECT
	APPT_I,
	APPT_ATTR_CLAS_C,
	SRCE_SYST_C,
	EFFT_D,
	EXPY_D,
	PROS_KEY_EXPY_I 
FROM {{ ref('xfm_fhb_foin__outFOIN_upd') }}