{{ config(materialized='view', tags=['DltAppt_Pdct_RpayFrmTMP']) }}

WITH GetGDWFields AS (
	SELECT
		{{ ref('Update__updates') }}.APPT_PDCT_I,
		{{ ref('Update__updates') }}.SRCE_SYST_C,
		{{ ref('gdw_cpy') }}.EFFT_D,
		{{ ref('Update__updates') }}.EXPY_D,
		{{ ref('Update__updates') }}.PROS_KEY_EXPY_I
	FROM {{ ref('Update__updates') }}
	INNER JOIN {{ ref('gdw_cpy') }} ON {{ ref('Update__updates') }}.APPT_PDCT_I = {{ ref('gdw_cpy') }}.APPT_PDCT_I
	AND {{ ref('Update__updates') }}.SRCE_SYST_C = {{ ref('gdw_cpy') }}.SRCE_SYST_C
)

SELECT * FROM GetGDWFields