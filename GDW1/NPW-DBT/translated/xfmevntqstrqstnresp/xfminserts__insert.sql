{{ config(materialized='view', tags=['XfmEvntQstrQstnResp']) }}

WITH XfmInserts__Insert AS (
	SELECT
		-- *SRC*: \(20)If IsNull(ToEvntRel.RESP_C) Then 'Y' Else  If Trim(ToEvntRel.RESP_C) = '' Then 'Y' Else 'N',
		IFF({{ ref('LeftOuterJoin') }}.RESP_C IS NULL, 'Y', IFF(TRIM({{ ref('LeftOuterJoin') }}.RESP_C) = '', 'Y', 'N')) AS svIsNullRespC,
		EVNT_I,
		QSTR_C,
		QSTN_C,
		-- *SRC*: \(20)If svIsNullRespC = 'N' Then ToEvntRel.RESP_C Else '9999',
		IFF(svIsNullRespC = 'N', {{ ref('LeftOuterJoin') }}.RESP_C, '9999') AS RESP_C,
		RESP_VALU_N,
		RESP_VALU_S,
		RESP_VALU_D,
		RESP_VALU_T,
		RESP_VALU_X,
		EFFT_D,
		EXPY_D,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		ROW_SECU_ACCS_C
	FROM {{ ref('LeftOuterJoin') }}
	WHERE {{ ref('LeftOuterJoin') }}.dummy = 0
)

SELECT * FROM XfmInserts__Insert