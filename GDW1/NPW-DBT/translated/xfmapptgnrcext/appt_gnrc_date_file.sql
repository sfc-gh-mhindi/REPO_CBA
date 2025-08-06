{{ config(materialized='incremental', alias='_cba__app01_csel4_dev_inprocess_cse__ccl__cli__date__exp__aud__cse__ccl__cli__date__exp__aud__appt__gnrc__date__temp', incremental_strategy='insert_overwrite', tags=['XfmApptGnrcEXT']) }}

SELECT
	APPT_I,
	DATE_ROLE_C,
	MODF_S,
	MODF_D,
	MODF_T,
	GNRC_ROLE_S,
	GNRC_ROLE_D,
	GNRC_ROLE_T,
	USER_I,
	CHNG_REAS_TYPE_C,
	promise_type 
FROM {{ ref('Transformer_32__tofile') }}