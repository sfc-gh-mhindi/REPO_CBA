{{ config(materialized='view', tags=['DltAppt_StusFrmTMP']) }}

WITH GetGDWFields AS (
	SELECT
		{{ ref('Update__updates') }}.APPT_I,
		{{ ref('gdw_cpy') }}.EFFT_D,
		{{ ref('Update__updates') }}.EXPY_D,
		{{ ref('Update__updates') }}.PROS_KEY_EXPY_I
	FROM {{ ref('Update__updates') }}
	LEFT JOIN {{ ref('gdw_cpy') }} ON {{ ref('Update__updates') }}.APPT_I = {{ ref('gdw_cpy') }}.APPT_I
)

SELECT * FROM GetGDWFields