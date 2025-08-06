{{ config(materialized='view', tags=['XfmUnidPatyGnrc']) }}

WITH Lk_BusRules AS (
	SELECT
		{{ ref('Xfm_BusinessRules') }}.UNID_PATY_I,
		{{ ref('Xfm_BusinessRules') }}.EFFT_D,
		{{ ref('Xfm_BusinessRules') }}.PATY_TYPE_C,
		{{ ref('Xfm_BusinessRules') }}.PATY_ROLE_C,
		{{ ref('Xfm_BusinessRules') }}.PROS_KEY_EFFT_I,
		{{ ref('Xfm_BusinessRules') }}.SRCE_SYST_C,
		{{ ref('Xfm_BusinessRules') }}.PATY_QLFY_C,
		{{ ref('Xfm_BusinessRules') }}.SRCE_SYST_PATY_I,
		To_UnidPatyGnrc.UNID_PATY_I,
		To_UnidPatyGnrc.EFFT_D,
		To_UnidPatyGnrc.PATY_TYPE_C,
		To_UnidPatyGnrc.PATY_ROLE_C,
		To_UnidPatyGnrc.PROS_KEY_EFFT_I,
		To_UnidPatyGnrc.SRCE_SYST_C,
		To_UnidPatyGnrc.PATY_QLFY_C,
		To_UnidPatyGnrc.SRCE_SYST_PATY_I
	FROM {{ ref('Xfm_BusinessRules') }}
	LEFT JOIN {{ ref('UnidPatyGnrc') }} ON {{ ref('Xfm_BusinessRules') }}.UNID_PATY_I = {{ ref('UnidPatyGnrc') }}.UNID_PATY_I
	AND {{ ref('Xfm_BusinessRules') }}.SRCE_SYST_PATY_I = {{ ref('UnidPatyGnrc') }}.SRCE_SYST_PATY_I
)

SELECT * FROM Lk_BusRules