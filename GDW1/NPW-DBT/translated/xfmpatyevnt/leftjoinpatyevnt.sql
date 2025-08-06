{{ config(materialized='view', tags=['XfmPatyEvnt']) }}

WITH LeftJoinPatyEvnt AS (
	SELECT
		{{ ref('XfmTrans') }}.EVNT_I,
		{{ ref('XfmTrans') }}.SRCE_SYST_PATY_I,
		{{ ref('XfmTrans') }}.EVNT_PATY_ROLE_TYPE_C,
		{{ ref('XfmTrans') }}.SRCE_SYST_C,
		{{ ref('XfmTrans') }}.EFFT_D,
		{{ ref('XfmTrans') }}.PATY_I,
		{{ ref('XfmTrans') }}.EXPY_D,
		{{ ref('XfmTrans') }}.PROS_KEY_EFFT_I,
		{{ ref('XfmTrans') }}.PROS_KEY_EXPY_I,
		{{ ref('XfmTrans') }}.EROR_SEQN_I,
		{{ ref('XfmTrans') }}.ROW_SECU_ACCS_C,
		{{ ref('GdwPatyEvnt') }}.dummy
	FROM {{ ref('XfmTrans') }}
	LEFT JOIN {{ ref('GdwPatyEvnt') }} ON {{ ref('XfmTrans') }}.PATY_I = {{ ref('GdwPatyEvnt') }}.PATY_I
)

SELECT * FROM LeftJoinPatyEvnt