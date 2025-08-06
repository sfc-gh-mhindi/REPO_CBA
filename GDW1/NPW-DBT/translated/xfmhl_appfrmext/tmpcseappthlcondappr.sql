{{ config(materialized='incremental', alias='_cba__app_pj__rapidresponseteam_csel4_dev_dataset_tmp__cse__chl__bus__app__cse__appt__hl__cond__appr', incremental_strategy='insert_overwrite', tags=['XfmHL_APPFrmExt']) }}

SELECT
	APPT_I,
	EFFT_D,
	COND_APPR_F,
	COND_APPR_CONV_TO_FULL_D,
	EXPY_D,
	ROW_SECU_ACCS_C 
FROM {{ ref('DeDup_CseApptHlCondAppr') }}