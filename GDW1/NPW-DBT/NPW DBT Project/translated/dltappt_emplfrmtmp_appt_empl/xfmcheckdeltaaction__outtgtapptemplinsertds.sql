{{ config(materialized='view', tags=['DltAPPT_EMPLFrmTMP_APPT_EMPL']) }}

WITH XfmCheckDeltaAction__OutTgtApptEmplInsertDS AS (
	SELECT
		-- *SRC*: DateFromDaysSince(-1, StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')) AS ExpiryDate,
		1 AS INSERT,
		3 AS UPDATE,
		-- *SRC*: ( IF IsNotNull((JoinAllApptEmpl.NEW_EMPL_I)) THEN (JoinAllApptEmpl.NEW_EMPL_I) ELSE ""),
		IFF({{ ref('JoinAll') }}.NEW_EMPL_I IS NOT NULL, {{ ref('JoinAll') }}.NEW_EMPL_I, '') AS EMPL_I,
		-- *SRC*: ( IF IsNotNull((JoinAllApptEmpl.NEW_APPT_I)) THEN (JoinAllApptEmpl.NEW_APPT_I) ELSE ""),
		IFF({{ ref('JoinAll') }}.NEW_APPT_I IS NOT NULL, {{ ref('JoinAll') }}.NEW_APPT_I, '') AS APPT_I,
		-- *SRC*: ( IF IsNotNull((JoinAllApptEmpl.NEW_EMPL_ROLE_C)) THEN (JoinAllApptEmpl.NEW_EMPL_ROLE_C) ELSE ""),
		IFF({{ ref('JoinAll') }}.NEW_EMPL_ROLE_C IS NOT NULL, {{ ref('JoinAll') }}.NEW_EMPL_ROLE_C, '') AS EMPL_ROLE_C,
		-- *SRC*: StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd'),
		STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd') AS EFFT_D,
		-- *SRC*: StringToDate('9999-12-31', '%yyyy-%mm-%dd'),
		STRINGTODATE('9999-12-31', '%yyyy-%mm-%dd') AS EXPY_D,
		REFR_PK AS PROS_KEY_EFFT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS PROS_KEY_EXPY_I,
		-- *SRC*: SetNull(),
		SETNULL() AS EROR_SEQN_I
	FROM {{ ref('JoinAll') }}
	WHERE {{ ref('JoinAll') }}.change_code = INSERT OR {{ ref('JoinAll') }}.change_code = UPDATE
)

SELECT * FROM XfmCheckDeltaAction__OutTgtApptEmplInsertDS