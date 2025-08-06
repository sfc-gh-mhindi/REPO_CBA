{{ config(materialized='incremental', alias='_cba__app_pj__rapidresponseteam_csel4_dev_dataset_tmp__cse__chl__bus__app__cse__appt__gnrc__date', incremental_strategy='insert_overwrite', tags=['XfmHL_APPFrmExt_NEW']) }}

SELECT
	APPT_I,
	DATE_ROLE_C,
	EFFT_D,
	GNRC_ROLE_S,
	GNRC_ROLE_D,
	GNRC_ROLE_T,
	EXPY_D,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I,
	EROR_SEQN_I,
	MODF_S,
	MODF_D,
	MODF_T,
	USER_I,
	CHNG_REAS_TYPE_C 
FROM {{ ref('DeDup_CseApptGnrcDate') }}