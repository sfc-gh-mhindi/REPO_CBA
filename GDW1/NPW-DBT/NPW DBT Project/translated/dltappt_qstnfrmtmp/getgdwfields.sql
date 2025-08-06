{{ config(materialized='view', tags=['DltAppt_QstnFrmTMP']) }}

WITH GetGDWFields AS (
	SELECT
		{{ ref('Update__updates') }}.APPT_I,
		{{ ref('Update__updates') }}.QSTN_C,
		{{ ref('gdw_cpy') }}.EFFT_D,
		{{ ref('Update__updates') }}.EXPY_D,
		{{ ref('Update__updates') }}.PROS_KEY_EXPY_I
	FROM {{ ref('Update__updates') }}
	LEFT JOIN {{ ref('gdw_cpy') }} ON {{ ref('Update__updates') }}.APPT_I = {{ ref('gdw_cpy') }}.APPT_I
	AND {{ ref('Update__updates') }}.QSTN_C = {{ ref('gdw_cpy') }}.QSTN_C
)

SELECT * FROM GetGDWFields