{{ config(materialized='view', tags=['DltAPPT_DEPTFrmTMP_APPT_DEPT']) }}

WITH XfmCheckDeltaAction__OutTgtApptDeptInsertDS AS (
	SELECT
		-- *SRC*: DateFromDaysSince(-1, StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')) AS ExpiryDate,
		1 AS INSERT,
		3 AS UPDATE,
		-- *SRC*: ( IF IsNotNull((OutJoin.NEW_APPT_I)) THEN (OutJoin.NEW_APPT_I) ELSE ""),
		IFF({{ ref('Join') }}.NEW_APPT_I IS NOT NULL, {{ ref('Join') }}.NEW_APPT_I, '') AS APPT_I,
		-- *SRC*: ( IF IsNotNull((OutJoin.NEW_DEPT_ROLE_C)) THEN (OutJoin.NEW_DEPT_ROLE_C) ELSE ""),
		IFF({{ ref('Join') }}.NEW_DEPT_ROLE_C IS NOT NULL, {{ ref('Join') }}.NEW_DEPT_ROLE_C, '') AS DEPT_ROLE_C,
		-- *SRC*: StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd'),
		STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd') AS EFFT_D,
		{{ ref('Join') }}.NEW_DEPT_I AS DEPT_I,
		-- *SRC*: StringToDate('9999-12-31', '%yyyy-%mm-%dd'),
		STRINGTODATE('9999-12-31', '%yyyy-%mm-%dd') AS EXPY_D,
		REFR_PK AS PROS_KEY_EFFT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS PROS_KEY_EXPY_I,
		-- *SRC*: SetNull(),
		SETNULL() AS EROR_SEQN_I
	FROM {{ ref('Join') }}
	WHERE {{ ref('Join') }}.change_code = INSERT OR {{ ref('Join') }}.change_code = UPDATE
)

SELECT * FROM XfmCheckDeltaAction__OutTgtApptDeptInsertDS