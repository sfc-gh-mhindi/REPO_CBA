{{ config(materialized='incremental', alias='_cba__app_pj__rapidresponseteam_csel4_dev_dataset_tmp__cse__chl__bus__app__cse__appt__attr__clas__assc', incremental_strategy='insert_overwrite', tags=['XfmHL_APPFrmExt_NEW']) }}

SELECT
	APPT_I,
	APPT_ATTR_CLAS_C,
	APPT_ATTR_CLAS_VALU_C,
	SRCE_SYST_C,
	EFFT_D,
	EXPY_D,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I,
	ROW_SECU_ACCS_C 
FROM {{ ref('Com_FOIN_FHB') }}