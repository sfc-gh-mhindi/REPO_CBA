{{ config(materialized='incremental', alias='_cba__app01_csel4_dev_dataset_appt__gnrc__date__i__cse__ccl__cli__date__exp__aud__20100512', incremental_strategy='insert_overwrite', tags=['DltApptGnrcDateCCL']) }}

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
FROM {{ ref('RemDup') }}