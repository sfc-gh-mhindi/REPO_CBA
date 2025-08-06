{{ config(materialized='view', tags=['XfmBusnEvnt']) }}

WITH LeftJoinBusnEvnt AS (
	SELECT
		{{ ref('XfmTrans') }}.EVNT_I,
		{{ ref('XfmTrans') }}.SRCE_SYST_EVNT_I,
		{{ ref('XfmTrans') }}.EVNT_ACTL_D,
		{{ ref('XfmTrans') }}.SRCE_SYST_C,
		{{ ref('XfmTrans') }}.EROR_SEQN_I,
		{{ ref('XfmTrans') }}.SRCE_SYST_EVNT_TYPE_I,
		{{ ref('XfmTrans') }}.EVNT_ACTL_T,
		{{ ref('XfmTrans') }}.ROW_SECU_ACCS_C,
		{{ ref('GdwBusnEvnt') }}.dummy
	FROM {{ ref('XfmTrans') }}
	LEFT JOIN {{ ref('GdwBusnEvnt') }} ON {{ ref('XfmTrans') }}.EVNT_I = {{ ref('GdwBusnEvnt') }}.EVNT_I
)

SELECT * FROM LeftJoinBusnEvnt