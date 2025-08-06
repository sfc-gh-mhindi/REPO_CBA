{{ config(materialized='view', tags=['XfmEvntAntn']) }}

WITH XfmInserts AS (
	SELECT
		EVNT_I,
		ANTN_I,
		SRCE_SYST_C,
		-- *SRC*: StringToDate(pRUN_STRM_PROS_D, "%yyyy%mm%dd"),
		STRINGTODATE(pRUN_STRM_PROS_D, '%yyyy%mm%dd') AS EFFT_D,
		-- *SRC*: StringToDate("99991231", "%yyyy%mm%dd"),
		STRINGTODATE('99991231', '%yyyy%mm%dd') AS EXPY_D,
		pGDW_PROS_ID AS PROS_KEY_EFFT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS PROS_KEY_EXPY_I,
		EROR_SEQN_I,
		ROW_SECU_ACCS_C
	FROM {{ ref('LeftJoinEvntAntn') }}
	WHERE {{ ref('LeftJoinEvntAntn') }}.dummy = '0'
)

SELECT * FROM XfmInserts