{{ config(materialized='view', tags=['DltAPPTFrmTMP_APPT']) }}

WITH XfmCheckDeltaAction__OutTgtApptInsertDS AS (
	SELECT
		-- *SRC*: DateFromDaysSince(-1, StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')) AS ExpiryDate,
		1 AS INSERT,
		3 AS UPDATE,
		-- *SRC*: ( IF IsNotNull((JoinAllAppt.NEW_APPT_I)) THEN (JoinAllAppt.NEW_APPT_I) ELSE ""),
		IFF({{ ref('JoinAll') }}.NEW_APPT_I IS NOT NULL, {{ ref('JoinAll') }}.NEW_APPT_I, '') AS APPT_I,
		{{ ref('JoinAll') }}.NEW_APPT_C AS APPT_C,
		{{ ref('JoinAll') }}.NEW_APPT_FORM_C AS APPT_FORM_C,
		{{ ref('JoinAll') }}.NEW_APPT_QLFY_C AS APPT_QLFY_C,
		{{ ref('JoinAll') }}.NEW_STUS_TRAK_I AS STUS_TRAK_I,
		{{ ref('JoinAll') }}.NEW_APPT_ORIG_C AS APPT_ORIG_C,
		{{ ref('JoinAll') }}.NEW_APPT_N AS APPT_N,
		{{ ref('JoinAll') }}.NEW_SRCE_SYST_C AS SRCE_SYST_C,
		{{ ref('JoinAll') }}.NEW_SRCE_SYST_APPT_I AS SRCE_SYST_APPT_I,
		{{ ref('JoinAll') }}.NEW_APPT_CRAT_D AS APPT_CRAT_D,
		{{ ref('JoinAll') }}.NEW_RATE_SEEK_F AS RATE_SEEK_F,
		REFR_PK AS PROS_KEY_EFFT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS EROR_SEQN_I
	FROM {{ ref('JoinAll') }}
	WHERE {{ ref('JoinAll') }}.change_code = INSERT
)

SELECT * FROM XfmCheckDeltaAction__OutTgtApptInsertDS