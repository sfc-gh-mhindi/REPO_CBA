{{ config(materialized='view', tags=['XfmApptGnrcEXT']) }}

WITH FunnelTrans AS (
	SELECT
		APPT_I as CHNG_REAS_TYPE_C,
		DATE_ROLE_C as APPT_I,
		MODF_S as DATE_ROLE_C,
		MODF_D as MODF_S,
		MODF_T as MODF_D,
		GNRC_ROLE_S as MODF_T,
		GNRC_ROLE_D as GNRC_ROLE_S,
		GNRC_ROLE_T as GNRC_ROLE_D,
		USER_I as USER_I,
		CHNG_REAS_TYPE_C as GNRC_ROLE_T,
		promise_type as promise_type,
		change_cat_id as change_cat_id,
		ccl_app_id as ccl_app_id,
		audit_date as audit_date,
		delivery_date as delivery_date
	FROM {{ ref('LookupFilt') }}
	UNION ALL
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
		delivery_date
	FROM {{ ref('FltChange') }}
)

SELECT * FROM FunnelTrans