{{ config(materialized='view', tags=['DltAPPT_ASES_DETLFrmTMP_APPT_ASES_DETL']) }}

WITH XfmCheckDeltaAction__OutTgtApptAsesDetlInsertDS AS (
	SELECT
		-- *SRC*: DateFromDaysSince(-1, StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')) AS ExpiryDate,
		1 AS INSERT,
		3 AS UPDATE,
		-- *SRC*: ( IF IsNotNull((JoinAllApptAsesDetl.NEW_APPT_I)) THEN (JoinAllApptAsesDetl.NEW_APPT_I) ELSE ""),
		IFF({{ ref('JoinAll') }}.NEW_APPT_I IS NOT NULL, {{ ref('JoinAll') }}.NEW_APPT_I, '') AS APPT_I,
		-- *SRC*: ( IF IsNotNull((JoinAllApptAsesDetl.NEW_AMT_TYPE_C)) THEN (JoinAllApptAsesDetl.NEW_AMT_TYPE_C) ELSE ""),
		IFF({{ ref('JoinAll') }}.NEW_AMT_TYPE_C IS NOT NULL, {{ ref('JoinAll') }}.NEW_AMT_TYPE_C, '') AS AMT_TYPE_C,
		-- *SRC*: StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd'),
		STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd') AS EFFT_D,
		-- *SRC*: StringToDate('9999-12-31', '%yyyy-%mm-%dd'),
		STRINGTODATE('9999-12-31', '%yyyy-%mm-%dd') AS EXPY_D,
		{{ ref('JoinAll') }}.NEW_CNCY_C AS CNCY_C,
		{{ ref('JoinAll') }}.NEW_APPT_ASES_A AS APPT_ASES_A,
		REFR_PK AS PROS_KEY_EFFT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS PROS_KEY_EXPY_I,
		-- *SRC*: SetNull(),
		SETNULL() AS EROR_SEQN_I
	FROM {{ ref('JoinAll') }}
	WHERE {{ ref('JoinAll') }}.change_code = INSERT OR {{ ref('JoinAll') }}.change_code = UPDATE
)

SELECT * FROM XfmCheckDeltaAction__OutTgtApptAsesDetlInsertDS