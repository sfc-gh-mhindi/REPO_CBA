{{ config(materialized='view', tags=['XfmEvntQstrQstnResp']) }}

WITH LeftOuterJoin AS (
	SELECT
		{{ ref('XfmTrans') }}.EVNT_I,
		{{ ref('XfmTrans') }}.QSTR_C,
		{{ ref('XfmTrans') }}.QSTN_C,
		{{ ref('XfmTrans') }}.RESP_C,
		{{ ref('XfmTrans') }}.RESP_VALU_N,
		{{ ref('XfmTrans') }}.RESP_VALU_S,
		{{ ref('XfmTrans') }}.RESP_VALU_D,
		{{ ref('XfmTrans') }}.RESP_VALU_T,
		{{ ref('XfmTrans') }}.RESP_VALU_X,
		{{ ref('XfmTrans') }}.EFFT_D,
		{{ ref('XfmTrans') }}.EXPY_D,
		{{ ref('XfmTrans') }}.PROS_KEY_EFFT_I,
		{{ ref('XfmTrans') }}.PROS_KEY_EXPY_I,
		{{ ref('XfmTrans') }}.ROW_SECU_ACCS_C,
		{{ ref('EVNT_QSTR_QSTN_RESP') }}.dummy,
		{{ ref('XfmTrans') }}.OL_CLIENT_RM_RATING_ID,
		{{ ref('XfmTrans') }}.RATING
	FROM {{ ref('XfmTrans') }}
	LEFT JOIN {{ ref('EVNT_QSTR_QSTN_RESP') }} ON {{ ref('XfmTrans') }}.EVNT_I = {{ ref('EVNT_QSTR_QSTN_RESP') }}.EVNT_I
)

SELECT * FROM LeftOuterJoin