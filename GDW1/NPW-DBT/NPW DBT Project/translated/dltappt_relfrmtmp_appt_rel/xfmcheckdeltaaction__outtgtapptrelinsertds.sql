{{ config(materialized='view', tags=['DltAPPT_RELFrmTMP_APPT_REL']) }}

WITH XfmCheckDeltaAction__OutTgtApptRelInsertDS AS (
	SELECT
		-- *SRC*: DateFromDaysSince(-1, StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')) AS ExpiryDate,
		1 AS INSERT,
		3 AS UPDATE,
		-- *SRC*: ( IF IsNotNull((JoinAllApptRel.NEW_APPT_I)) THEN (JoinAllApptRel.NEW_APPT_I) ELSE ""),
		IFF({{ ref('JoinAll') }}.NEW_APPT_I IS NOT NULL, {{ ref('JoinAll') }}.NEW_APPT_I, '') AS APPT_I,
		-- *SRC*: ( IF IsNotNull((JoinAllApptRel.NEW_RELD_APPT_I)) THEN (JoinAllApptRel.NEW_RELD_APPT_I) ELSE ""),
		IFF({{ ref('JoinAll') }}.NEW_RELD_APPT_I IS NOT NULL, {{ ref('JoinAll') }}.NEW_RELD_APPT_I, '') AS RELD_APPT_I,
		-- *SRC*: ( IF IsNotNull((JoinAllApptRel.NEW_REL_TYPE_C)) THEN (JoinAllApptRel.NEW_REL_TYPE_C) ELSE ""),
		IFF({{ ref('JoinAll') }}.NEW_REL_TYPE_C IS NOT NULL, {{ ref('JoinAll') }}.NEW_REL_TYPE_C, '') AS REL_TYPE_C,
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

SELECT * FROM XfmCheckDeltaAction__OutTgtApptRelInsertDS