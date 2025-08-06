{{ config(materialized='view', tags=['XfmEvntAntn']) }}

WITH LeftJoinEvntAntn AS (
	SELECT
		{{ ref('XfmTrans') }}.EVNT_I,
		{{ ref('XfmTrans') }}.ANTN_I,
		{{ ref('XfmTrans') }}.SRCE_SYST_C,
		{{ ref('XfmTrans') }}.EROR_SEQN_I,
		{{ ref('XfmTrans') }}.ROW_SECU_ACCS_C,
		{{ ref('GdwEvntAntn') }}.dummy
	FROM {{ ref('XfmTrans') }}
	LEFT JOIN {{ ref('GdwEvntAntn') }} ON {{ ref('XfmTrans') }}.EVNT_I = {{ ref('GdwEvntAntn') }}.EVNT_I
	AND {{ ref('XfmTrans') }}.ANTN_I = {{ ref('GdwEvntAntn') }}.ANTN_I
)

SELECT * FROM LeftJoinEvntAntn