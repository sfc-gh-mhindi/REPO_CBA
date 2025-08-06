{{ config(materialized='view', tags=['DltAPPTFrmTMP_APPT']) }}

WITH XfmCheckDeltaAction__OutTgtApptUpdateDS AS (
	SELECT
		-- *SRC*: DateFromDaysSince(-1, StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')) AS ExpiryDate,
		1 AS INSERT,
		3 AS UPDATE,
		-- *SRC*: ( IF IsNotNull((JoinAllAppt.NEW_APPT_I)) THEN (JoinAllAppt.NEW_APPT_I) ELSE ""),
		IFF({{ ref('JoinAll') }}.NEW_APPT_I IS NOT NULL, {{ ref('JoinAll') }}.NEW_APPT_I, '') AS APPT_I,
		{{ ref('JoinAll') }}.NEW_APPT_C AS APPT_C,
		{{ ref('JoinAll') }}.NEW_APPT_FORM_C AS APPT_FORM_C,
		{{ ref('JoinAll') }}.NEW_STUS_TRAK_I AS STUS_TRAK_I,
		{{ ref('JoinAll') }}.NEW_APPT_ORIG_C AS APPT_ORIG_C
	FROM {{ ref('JoinAll') }}
	WHERE {{ ref('JoinAll') }}.change_code = UPDATE
)

SELECT * FROM XfmCheckDeltaAction__OutTgtApptUpdateDS