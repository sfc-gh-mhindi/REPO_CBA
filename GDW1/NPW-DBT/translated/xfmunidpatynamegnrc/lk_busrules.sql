{{ config(materialized='view', tags=['XfmUnidPatyNameGnrc']) }}

WITH Lk_BusRules AS (
	SELECT
		{{ ref('Xfm_BusinRules') }}.UNID_PATY_I,
		{{ ref('Xfm_BusinRules') }}.PATY_NAME_TYPE_C,
		{{ ref('Xfm_BusinRules') }}.EFFT_D,
		{{ ref('Xfm_BusinRules') }}.PATY_ROLE_C,
		{{ ref('Xfm_BusinRules') }}.PROS_KEY_EFFT_I,
		{{ ref('Xfm_BusinRules') }}.SRCE_SYST_C,
		{{ ref('Xfm_BusinRules') }}.SALU_C,
		{{ ref('Xfm_BusinRules') }}.TITL_C,
		{{ ref('Xfm_BusinRules') }}.FRST_M,
		{{ ref('Xfm_BusinRules') }}.SCND_M,
		{{ ref('Xfm_BusinRules') }}.SRNM_M,
		{{ ref('Xfm_BusinRules') }}.THRD_M,
		{{ ref('Xfm_BusinRules') }}.FRTH_M,
		{{ ref('Xfm_BusinRules') }}.SUF_C,
		{{ ref('Xfm_BusinRules') }}.EXPY_D,
		{{ ref('Xfm_BusinRules') }}.PROS_KEY_EXPY_I,
		{{ ref('Xfm_BusinRules') }}.EROR_SEQN_I,
		{{ ref('Xfm_BusinRules') }}.CO_CTCT_FRST_M,
		{{ ref('Xfm_BusinRules') }}.CO_CTCT_LAST_M,
		{{ ref('Xfm_BusinRules') }}.CO_CTCT_PRFR_M
	FROM {{ ref('Xfm_BusinRules') }}
	LEFT JOIN {{ ref('UnidPatyNameGnrc') }} ON {{ ref('Xfm_BusinRules') }}.UNID_PATY_I = {{ ref('UnidPatyNameGnrc') }}.UNID_PATY_I
	AND {{ ref('Xfm_BusinRules') }}.SRCE_SYST_C = {{ ref('UnidPatyNameGnrc') }}.SRCE_SYST_C
)

SELECT * FROM Lk_BusRules