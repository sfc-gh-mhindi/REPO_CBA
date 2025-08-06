{{ config(materialized='view', tags=['XfmApptGnrcEXT']) }}

WITH LookupFilt AS (
	SELECT
		{{ ref('FltChange') }}.APPT_I,
		{{ ref('FltChange') }}.DATE_ROLE_C,
		{{ ref('FltChange') }}.MODF_S,
		{{ ref('FltChange') }}.MODF_D,
		{{ ref('FltChange') }}.MODF_T,
		{{ ref('FltChange') }}.GNRC_ROLE_S,
		{{ ref('FltChange') }}.GNRC_ROLE_D,
		{{ ref('FltChange') }}.GNRC_ROLE_T,
		{{ ref('FltChange') }}.USER_I,
		{{ ref('MAP_CSE_APPT_GNRC_DATE_CR') }}.CHNG_REAS_TYPE_C,
		{{ ref('FltChange') }}.promise_type,
		{{ ref('FltChange') }}.change_cat_id,
		{{ ref('FltChange') }}.ccl_app_id,
		{{ ref('FltChange') }}.audit_date,
		{{ ref('FltChange') }}.delivery_date
	FROM {{ ref('FltChange') }}
	LEFT JOIN {{ ref('MAP_CSE_APPT_GNRC_DATE_CR') }} ON {{ ref('FltChange') }}.promise_type = {{ ref('MAP_CSE_APPT_GNRC_DATE_CR') }}.Promise_Type
	AND {{ ref('FltChange') }}.change_cat_id = {{ ref('MAP_CSE_APPT_GNRC_DATE_CR') }}.change_cat_id
	WHERE CHNG_REAS_TYPE_C = 'Y'
)

SELECT * FROM LookupFilt