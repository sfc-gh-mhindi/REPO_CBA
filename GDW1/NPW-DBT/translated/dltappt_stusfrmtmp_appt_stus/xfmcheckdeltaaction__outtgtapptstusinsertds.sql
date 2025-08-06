{{ config(materialized='view', tags=['DltAPPT_STUSFrmTMP_APPT_STUS']) }}

WITH XfmCheckDeltaAction__OutTgtApptStusInsertDS AS (
	SELECT
		-- *SRC*: DateFromDaysSince(-1, StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')) AS ExpiryDate,
		1 AS INSERT,
		3 AS UPDATE,
		-- *SRC*: ( IF IsNotNull((JoinAllApptStus.NEW_APPT_I)) THEN (JoinAllApptStus.NEW_APPT_I) ELSE ""),
		IFF({{ ref('JoinAll') }}.NEW_APPT_I IS NOT NULL, {{ ref('JoinAll') }}.NEW_APPT_I, '') AS APPT_I,
		-- *SRC*: ( IF IsNotNull((JoinAllApptStus.NEW_STUS_C)) THEN (JoinAllApptStus.NEW_STUS_C) ELSE ""),
		IFF({{ ref('JoinAll') }}.NEW_STUS_C IS NOT NULL, {{ ref('JoinAll') }}.NEW_STUS_C, '') AS STUS_C,
		-- *SRC*: ( IF IsNotNull((JoinAllApptStus.NEW_STRT_S)) THEN (JoinAllApptStus.NEW_STRT_S) ELSE ""),
		IFF({{ ref('JoinAll') }}.NEW_STRT_S IS NOT NULL, {{ ref('JoinAll') }}.NEW_STRT_S, '') AS STRT_S,
		{{ ref('JoinAll') }}.NEW_STRT_D AS STRT_D,
		{{ ref('JoinAll') }}.NEW_STRT_T AS STRT_T,
		{{ ref('JoinAll') }}.NEW_END_D AS END_D,
		{{ ref('JoinAll') }}.NEW_END_T AS END_T,
		{{ ref('JoinAll') }}.NEW_END_S AS END_S,
		{{ ref('JoinAll') }}.NEW_EMPL_I AS EMPL_I,
		-- *SRC*: StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd'),
		STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd') AS EFFT_D,
		-- *SRC*: \(20)If IsNull(JoinAllApptStus.NEW_END_S) then StringToDate('9999-12-31', '%yyyy-%mm-%dd') Else StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd'),
		IFF({{ ref('JoinAll') }}.NEW_END_S IS NULL, STRINGTODATE('9999-12-31', '%yyyy-%mm-%dd'), STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')) AS EXPY_D,
		REFR_PK AS PROS_KEY_EFFT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS PROS_KEY_EXPY_I,
		-- *SRC*: SetNull(),
		SETNULL() AS EROR_SEQN_I
	FROM {{ ref('JoinAll') }}
	WHERE {{ ref('JoinAll') }}.change_code = INSERT OR {{ ref('JoinAll') }}.change_code = UPDATE
)

SELECT * FROM XfmCheckDeltaAction__OutTgtApptStusInsertDS