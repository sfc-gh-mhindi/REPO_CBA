{{ config(materialized='view', tags=['DltApptGnrcDateCCL']) }}

WITH 
_cba__app01_csel4_dev_inprocess_____var__dbt__pfilename______cse__ccl__cli__date__exp__aud__appt__gnrc__date AS (
	SELECT
	*
	FROM {{ source("","_cba__app01_csel4_dev_inprocess_____var__dbt__pfilename______cse__ccl__cli__date__exp__aud__appt__gnrc__date")  }})
rs_APPT_GNRC_DATE AS (
	SELECT APPT_I,
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
	FROM _cba__app01_csel4_dev_inprocess_____var__dbt__pfilename______cse__ccl__cli__date__exp__aud__appt__gnrc__date
)

SELECT * FROM rs_APPT_GNRC_DATE