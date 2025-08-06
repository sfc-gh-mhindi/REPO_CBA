{{ config(materialized='view', tags=['XfmApptGnrcEXT']) }}

WITH LkpDateRoleC AS (
	SELECT
		{{ ref('MAP_CSE_APPT_GNRC_DATE_DR') }}.DATE_ROLE_C,
		{{ ref('XfmFilter') }}.ccl_app_id,
		{{ ref('XfmFilter') }}.audit_date,
		{{ ref('XfmFilter') }}.delivery_date,
		{{ ref('XfmFilter') }}.mod_user_id,
		{{ ref('XfmFilter') }}.change_cat_id,
		{{ ref('XfmFilter') }}.promise_type
	FROM {{ ref('XfmFilter') }}
	LEFT JOIN {{ ref('MAP_CSE_APPT_GNRC_DATE_DR') }} ON {{ ref('XfmFilter') }}.promise_type = {{ ref('MAP_CSE_APPT_GNRC_DATE_DR') }}.Promise_Type
)

SELECT * FROM LkpDateRoleC