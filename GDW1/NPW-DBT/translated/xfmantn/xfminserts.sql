{{ config(materialized='view', tags=['XfmAntn']) }}

WITH XfmInserts AS (
	SELECT
		SvLoadEvnt,
		ANTN_I,
		ANTN_TYPE_C,
		ANTN_X,
		SRCE_SYST_C,
		SRCE_SYST_ANTN_I,
		ANTN_S,
		ANTN_D,
		ANTN_T,
		EMPL_I,
		USER_I,
		PRVT_F,
		-- *SRC*: StringToDate(pRUN_STRM_PROS_D, "%yyyy%mm%dd"),
		STRINGTODATE(pRUN_STRM_PROS_D, '%yyyy%mm%dd') AS EFFT_D,
		-- *SRC*: StringToDate("99991231", "%yyyy%mm%dd"),
		STRINGTODATE('99991231', '%yyyy%mm%dd') AS EXPY_D,
		pGDW_PROS_ID AS PROS_KEY_EFFT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS PROS_KEY_EXPY_I,
		EROR_SEQN_I
	FROM {{ ref('LeftJoinAntn') }}
	WHERE {{ ref('LeftJoinAntn') }}.dummy = '0'
)

SELECT * FROM XfmInserts