{{ config(materialized='view', tags=['DltApptPdctFeatFrmTMP']) }}

WITH GetGDWFields AS (
	SELECT
		{{ ref('Update__updates') }}.APPT_PDCT_I,
		{{ ref('Update__updates') }}.FEAT_I,
		{{ ref('Update__updates') }}.SRCE_SYST_C,
		{{ ref('gdw_cpy') }}.EFFT_D,
		{{ ref('Update__updates') }}.EXPY_D,
		{{ ref('Update__updates') }}.PROS_KEY_EXPY_I
	FROM {{ ref('Update__updates') }}
	LEFT JOIN {{ ref('gdw_cpy') }} ON {{ ref('Update__updates') }}.APPT_PDCT_I = {{ ref('gdw_cpy') }}.APPT_PDCT_I
	AND {{ ref('Update__updates') }}.FEAT_I = {{ ref('gdw_cpy') }}.FEAT_I
	AND {{ ref('Update__updates') }}.SRCE_SYST_C = {{ ref('gdw_cpy') }}.SRCE_SYST_C
)

SELECT * FROM GetGDWFields