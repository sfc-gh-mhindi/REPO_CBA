{{ config(materialized='view', tags=['XfmApptGnrcEXT']) }}

WITH FltChange AS (
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
		promise_type,
		change_cat_id,
		ccl_app_id,
		audit_date,
		delivery_date,
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
		promise_type,
		change_cat_id,
		ccl_app_id,
		audit_date,
		delivery_date
	FROM {{ ref('Xfm') }}
)

SELECT * FROM FltChange