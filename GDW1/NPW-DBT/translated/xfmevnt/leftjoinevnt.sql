{{ config(materialized='view', tags=['XfmEvnt']) }}

WITH LeftJoinEvnt AS (
	SELECT
		{{ ref('XFM1') }}.EVNT_I,
		{{ ref('XFM1') }}.EVNT_ACTV_TYPE_C,
		{{ ref('XFM1') }}.INVT_EVNT_F,
		{{ ref('XFM1') }}.FNCL_ACCT_EVNT_F,
		{{ ref('XFM1') }}.CTCT_EVNT_F,
		{{ ref('XFM1') }}.BUSN_EVNT_F,
		{{ ref('XFM1') }}.EROR_SEQN_I,
		{{ ref('XFM1') }}.FNCL_NVAL_EVNT_F,
		{{ ref('XFM1') }}.INCD_F,
		{{ ref('XFM1') }}.INSR_EVNT_F,
		{{ ref('XFM1') }}.INSR_NVAL_EVNT_F,
		{{ ref('XFM1') }}.ROW_SECU_ACCS_C,
		{{ ref('XFM1') }}.FNCL_GL_EVNT_F,
		{{ ref('XFM1') }}.AUTT_AUTN_EVNT_F,
		{{ ref('XFM1') }}.COLL_EVNT_F,
		{{ ref('GdwEvnt') }}.dummy
	FROM {{ ref('XFM1') }}
	LEFT JOIN {{ ref('GdwEvnt') }} ON {{ ref('XFM1') }}.EVNT_I = {{ ref('GdwEvnt') }}.EVNT_I
)

SELECT * FROM LeftJoinEvnt