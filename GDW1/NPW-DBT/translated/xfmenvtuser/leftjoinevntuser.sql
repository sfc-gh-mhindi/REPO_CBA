{{ config(materialized='view', tags=['XfmEnvtUser']) }}

WITH LeftJoinEvntUser AS (
	SELECT
		{{ ref('XfmTrans') }}.EVNT_I,
		{{ ref('XfmTrans') }}.USER_I,
		{{ ref('XfmTrans') }}.EVNT_PATY_ROLE_C,
		{{ ref('XfmTrans') }}.EFFT_D,
		{{ ref('XfmTrans') }}.EXPY_D,
		{{ ref('XfmTrans') }}.PROS_KEY_EFFT_I,
		{{ ref('XfmTrans') }}.PROS_KEY_EXPY_I,
		{{ ref('XfmTrans') }}.EROR_SEQN_I,
		{{ ref('GdwEvntUser') }}.dummy
	FROM {{ ref('XfmTrans') }}
	LEFT JOIN {{ ref('GdwEvntUser') }} ON {{ ref('XfmTrans') }}.EVNT_I = {{ ref('GdwEvntUser') }}.EVNT_I
)

SELECT * FROM LeftJoinEvntUser