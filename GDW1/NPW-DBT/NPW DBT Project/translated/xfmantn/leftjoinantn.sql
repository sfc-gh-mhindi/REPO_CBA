{{ config(materialized='view', tags=['XfmAntn']) }}

WITH LeftJoinAntn AS (
	SELECT
		{{ ref('XfmTrans') }}.ANTN_I,
		{{ ref('XfmTrans') }}.ANTN_TYPE_C,
		{{ ref('XfmTrans') }}.ANTN_X,
		{{ ref('XfmTrans') }}.SRCE_SYST_C,
		{{ ref('XfmTrans') }}.SRCE_SYST_ANTN_I,
		{{ ref('XfmTrans') }}.ANTN_S,
		{{ ref('XfmTrans') }}.ANTN_D,
		{{ ref('XfmTrans') }}.ANTN_T,
		{{ ref('XfmTrans') }}.EMPL_I,
		{{ ref('XfmTrans') }}.USER_I,
		{{ ref('XfmTrans') }}.PRVT_F,
		{{ ref('XfmTrans') }}.EROR_SEQN_I,
		{{ ref('ANTN') }}.dummy
	FROM {{ ref('XfmTrans') }}
	LEFT JOIN {{ ref('ANTN') }} ON {{ ref('XfmTrans') }}.ANTN_I = {{ ref('ANTN') }}.ANTN_I
)

SELECT * FROM LeftJoinAntn