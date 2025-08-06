{{ config(materialized='incremental', alias='_cba__app01_csel4_dev_inprocess_cse__ccl__cli__date__exp__aud__cse__ccl__cli__date__exp__aud__appt__gnrc__date', incremental_strategy='insert_overwrite', tags=['XfmApptGnrcFile']) }}

SELECT
	APPT_I,
	DATE_ROLE_C,
	EFFT_D,
	GNRC_ROLE_S,
	GNRC_ROLE_D,
	GNRC_ROLE_T,
	PROS_KEY_EFFT_I,
	EROR_SEQN_I,
	MODF_S,
	MODF_D,
	MODF_T,
	USER_I,
	CHNG_REAS_TYPE_C,
	MODF_S_TEMP 
FROM {{ ref('TransTgt') }}