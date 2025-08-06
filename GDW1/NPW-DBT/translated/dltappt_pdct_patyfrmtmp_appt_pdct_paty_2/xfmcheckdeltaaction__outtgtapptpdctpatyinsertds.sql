{{ config(materialized='view', tags=['DltAPPT_PDCT_PATYFrmTMP_APPT_PDCT_PATY_2']) }}

WITH XfmCheckDeltaAction__OutTgtApptPdctPatyInsertDS AS (
	SELECT
		-- *SRC*: DateFromDaysSince(-1, StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')) AS ExpiryDate,
		1 AS INSERT,
		3 AS UPDATE,
		-- *SRC*: ( IF IsNotNull((JoinAllApptPdctpaty.NEW_APPT_PDCT_I)) THEN (JoinAllApptPdctpaty.NEW_APPT_PDCT_I) ELSE ""),
		IFF({{ ref('JoinAll') }}.NEW_APPT_PDCT_I IS NOT NULL, {{ ref('JoinAll') }}.NEW_APPT_PDCT_I, '') AS APPT_PDCT_I,
		-- *SRC*: ( IF IsNotNull((JoinAllApptPdctpaty.NEW_PATY_I)) THEN (JoinAllApptPdctpaty.NEW_PATY_I) ELSE ""),
		IFF({{ ref('JoinAll') }}.NEW_PATY_I IS NOT NULL, {{ ref('JoinAll') }}.NEW_PATY_I, '') AS PATY_I,
		-- *SRC*: ( IF IsNotNull((JoinAllApptPdctpaty.NEW_PATY_ROLE_C)) THEN (JoinAllApptPdctpaty.NEW_PATY_ROLE_C) ELSE ""),
		IFF({{ ref('JoinAll') }}.NEW_PATY_ROLE_C IS NOT NULL, {{ ref('JoinAll') }}.NEW_PATY_ROLE_C, '') AS PATY_ROLE_C,
		-- *SRC*: StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd'),
		STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd') AS EFFT_D,
		{{ ref('JoinAll') }}.NEW_SRCE_SYST_C AS SRCE_SYST_C,
		{{ ref('JoinAll') }}.NEW_SRCE_SYST_APPT_PDCT_PATY_I AS SRCE_SYST_APPT_PDCT_PATY_I,
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

SELECT * FROM XfmCheckDeltaAction__OutTgtApptPdctPatyInsertDS