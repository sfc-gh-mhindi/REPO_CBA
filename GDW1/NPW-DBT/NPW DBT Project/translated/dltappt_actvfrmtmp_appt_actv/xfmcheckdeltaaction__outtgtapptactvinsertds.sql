{{ config(materialized='view', tags=['DltAPPT_ACTVFrmTMP_APPT_ACTV']) }}

WITH XfmCheckDeltaAction__OutTgtApptActvInsertDS AS (
	SELECT
		-- *SRC*: DateFromDaysSince(-1, StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')) AS ExpiryDate,
		1 AS INSERT,
		3 AS UPDATE,
		{{ ref('JoinAll') }}.NEW_APPT_I AS APPT_I,
		'PRNT' AS APPT_ACTV_TYPE_C,
		'CSE' AS SRCE_SYST_C,
		{{ ref('JoinAll') }}.NEW_APPT_ACTV_Q AS APPT_ACTV_Q,
		-- *SRC*: StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd'),
		STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd') AS EFFT_D,
		-- *SRC*: StringToDate('9999-12-31', '%yyyy-%mm-%dd'),
		STRINGTODATE('9999-12-31', '%yyyy-%mm-%dd') AS EXPY_D,
		REFR_PK AS PROS_KEY_EFFT_I,
		-- *SRC*: setnull(),
		SETNULL() AS PROS_KEY_EXPY_I,
		0 AS ROW_SECU_ACCS_C
	FROM {{ ref('JoinAll') }}
	WHERE {{ ref('JoinAll') }}.change_code = INSERT OR {{ ref('JoinAll') }}.change_code = UPDATE
)

SELECT * FROM XfmCheckDeltaAction__OutTgtApptActvInsertDS