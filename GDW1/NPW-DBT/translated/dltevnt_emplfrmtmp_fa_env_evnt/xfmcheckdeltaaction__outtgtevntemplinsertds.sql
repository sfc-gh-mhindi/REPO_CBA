{{ config(materialized='view', tags=['DltEVNT_EMPLFrmTMP_FA_ENV_EVNT']) }}

WITH XfmCheckDeltaAction__OutTgtEvntEmplInsertDS AS (
	SELECT
		-- *SRC*: DateFromDaysSince(-1, StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')) AS ExpiryDate,
		1 AS INSERT,
		3 AS UPDATE,
		EVNT_I,
		ROW_SECU_ACCS_C,
		EMPL_I,
		EVNT_PATY_ROLE_TYPE_C,
		-- *SRC*: StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd'),
		STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd') AS EFFT_D,
		-- *SRC*: StringToDate('99991231', '%yyyy%mm%dd'),
		STRINGTODATE('99991231', '%yyyy%mm%dd') AS EXPY_D,
		REFR_PK AS PROS_KEY_EFFT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS PROS_KEY_EXPY_I,
		-- *SRC*: SetNull(),
		SETNULL() AS EROR_SEQN_I
	FROM {{ ref('Join_82') }}
	WHERE {{ ref('Join_82') }}.change_code = INSERT OR {{ ref('Join_82') }}.change_code = UPDATE
)

SELECT * FROM XfmCheckDeltaAction__OutTgtEvntEmplInsertDS