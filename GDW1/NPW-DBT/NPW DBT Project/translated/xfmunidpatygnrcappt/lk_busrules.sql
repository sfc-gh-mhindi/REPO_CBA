{{ config(materialized='view', tags=['XfmUnidPatyGnrcAppt']) }}

WITH Lk_BusRules AS (
	SELECT
		{{ ref('Xfm_BusinRules') }}.UNID_PATY_I,
		{{ ref('Xfm_BusinRules') }}.APPT_I,
		{{ ref('Xfm_BusinRules') }}.EFFT_D,
		{{ ref('Xfm_BusinRules') }}.EXPY_D,
		{{ ref('Xfm_BusinRules') }}.REL_TYPE_C,
		{{ ref('Xfm_BusinRules') }}.REL_REAS_C,
		{{ ref('Xfm_BusinRules') }}.REL_STUS_C,
		{{ ref('Xfm_BusinRules') }}.REL_LEVL_C,
		{{ ref('Xfm_BusinRules') }}.SRCE_SYST_C,
		{{ ref('Xfm_BusinRules') }}.PROS_KEY_EFFT_I,
		{{ ref('Xfm_BusinRules') }}.PROS_KEY_EXPY_I,
		{{ ref('Xfm_BusinRules') }}.ROW_SECU_ACCS_C,
		To_UnidPatyGnrc.UNID_PATY_I,
		To_UnidPatyGnrc.EFFT_D,
		To_UnidPatyGnrc.PATY_TYPE_C,
		To_UnidPatyGnrc.PATY_ROLE_C,
		To_UnidPatyGnrc.PROS_KEY_EFFT_I,
		To_UnidPatyGnrc.SRCE_SYST_C,
		To_UnidPatyGnrc.PATY_QLFY_C,
		To_UnidPatyGnrc.SRCE_SYST_PATY_I
	FROM {{ ref('Xfm_BusinRules') }}
	LEFT JOIN {{ ref('UnidPatyGnrcAppt') }} ON {{ ref('Xfm_BusinRules') }}.APPT_I = {{ ref('UnidPatyGnrcAppt') }}.APPT_I
	AND {{ ref('Xfm_BusinRules') }}.UNID_PATY_I = {{ ref('UnidPatyGnrcAppt') }}.UNID_PATY_I
	AND {{ ref('Xfm_BusinRules') }}.SRCE_SYST_C = {{ ref('UnidPatyGnrcAppt') }}.SRCE_SYST_C
)

SELECT * FROM Lk_BusRules