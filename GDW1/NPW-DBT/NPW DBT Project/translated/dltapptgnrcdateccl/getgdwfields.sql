{{ config(materialized='view', tags=['DltApptGnrcDateCCL']) }}

WITH GetGDWFields AS (
	SELECT
		{{ ref('DetermineChange__updates') }}.APPT_I,
		{{ ref('DetermineChange__updates') }}.DATE_ROLE_C,
		{{ ref('DetermineChange__updates') }}.EXPY_D,
		{{ ref('DetermineChange__updates') }}.PROS_KEY_EXPY_I,
		{{ ref('Copy') }}.EFFT_D
	FROM {{ ref('DetermineChange__updates') }}
	LEFT JOIN {{ ref('Copy') }} ON {{ ref('DetermineChange__updates') }}.APPT_I = {{ ref('Copy') }}.APPT_I
	AND {{ ref('DetermineChange__updates') }}.DATE_ROLE_C = {{ ref('Copy') }}.DATE_ROLE_C
)

SELECT * FROM GetGDWFields