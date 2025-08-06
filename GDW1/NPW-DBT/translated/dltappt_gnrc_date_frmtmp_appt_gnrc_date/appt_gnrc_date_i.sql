{{ config(materialized='incremental', alias='_cba__app_csel4_dev_dataset_appt__gnrc__date__i__cse__chl__bus__hlm__app__20100728', incremental_strategy='insert_overwrite', tags=['DltAPPT_GNRC_DATE_FrmTMP_APPT_GNRC_DATE']) }}

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
FROM {{ ref('DetermineChange__ToInserts') }}