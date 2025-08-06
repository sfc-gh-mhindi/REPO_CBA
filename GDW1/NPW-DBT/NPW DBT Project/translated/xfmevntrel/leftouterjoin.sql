{{ config(materialized='view', tags=['XfmEvntRel']) }}

WITH LeftOuterJoin AS (
	SELECT
		{{ ref('XfmTrans__ToJoin') }}.EVNT_I,
		{{ ref('XfmTrans__ToJoin') }}.RELD_EVNT_I,
		{{ ref('XfmTrans__ToJoin') }}.EFFT_D,
		{{ ref('XfmTrans__ToJoin') }}.EVNT_REL_TYPE_C,
		{{ ref('XfmTrans__ToJoin') }}.EXPY_D,
		{{ ref('XfmTrans__ToJoin') }}.PROS_KEY_EFFT_I,
		{{ ref('XfmTrans__ToJoin') }}.PROS_KEY_EXPY_I,
		{{ ref('XfmTrans__ToJoin') }}.EROR_SEQN_I,
		{{ ref('XfmTrans__ToJoin') }}.ROW_SECU_ACCS_C,
		{{ ref('EVNT_REL') }}.dummy
	FROM {{ ref('XfmTrans__ToJoin') }}
	LEFT JOIN {{ ref('EVNT_REL') }} ON {{ ref('XfmTrans__ToJoin') }}.EVNT_I = {{ ref('EVNT_REL') }}.EVNT_I
)

SELECT * FROM LeftOuterJoin