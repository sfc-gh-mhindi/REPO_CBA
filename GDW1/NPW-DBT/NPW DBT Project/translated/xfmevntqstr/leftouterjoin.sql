{{ config(materialized='view', tags=['XfmEvntQstr']) }}

WITH LeftOuterJoin AS (
	SELECT
		{{ ref('XfmTrans') }}.EVNT_I,
		{{ ref('XfmTrans') }}.QSTR_C,
		{{ ref('XfmTrans') }}.EFFT_D,
		{{ ref('XfmTrans') }}.EXPY_D,
		{{ ref('XfmTrans') }}.PROS_KEY_EFFT_I,
		{{ ref('XfmTrans') }}.PROS_KEY_EXPY_I,
		{{ ref('EVNT_QSTR') }}.dummy
	FROM {{ ref('XfmTrans') }}
	LEFT JOIN {{ ref('EVNT_QSTR') }} ON {{ ref('XfmTrans') }}.EVNT_I = {{ ref('EVNT_QSTR') }}.EVNT_I
)

SELECT * FROM LeftOuterJoin