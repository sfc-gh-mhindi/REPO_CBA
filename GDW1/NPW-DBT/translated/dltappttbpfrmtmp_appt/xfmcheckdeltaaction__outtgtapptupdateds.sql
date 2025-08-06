{{ config(materialized='view', tags=['DltAPPTTBPFrmTMP_APPT']) }}

WITH XfmCheckDeltaAction__OutTgtApptUpdateDS AS (
	SELECT
		1 AS INSERT,
		3 AS UPDATE,
		-- *SRC*: ( IF IsNotNull((JoinAllAppt.NEW_APPT_I)) THEN (JoinAllAppt.NEW_APPT_I) ELSE ""),
		IFF({{ ref('JoinAll') }}.NEW_APPT_I IS NOT NULL, {{ ref('JoinAll') }}.NEW_APPT_I, '') AS APPT_I,
		{{ ref('JoinAll') }}.NEW_APPT_C AS APPT_C,
		{{ ref('JoinAll') }}.NEW_APPT_FORM_C AS APPT_FORM_C,
		{{ ref('JoinAll') }}.NEW_STUS_TRAK_I AS STUS_TRAK_I,
		{{ ref('JoinAll') }}.NEW_APPT_ORIG_C AS APPT_ORIG_C,
		{{ ref('JoinAll') }}.NEW_APPT_ORIG_SYST_C AS ORIG_APPT_SRCE_C,
		{{ ref('JoinAll') }}.NEW_APPT_RECV_S AS APPT_RECV_S,
		{{ ref('JoinAll') }}.NEW_REL_MGR_STAT_C AS REL_MGR_STAT_C,
		{{ ref('JoinAll') }}.NEW_APPT_RECV_D AS APPT_RECV_D,
		-- *SRC*: \(20)if len(trim(( IF IsNotNull((JoinAllAppt.NEW_APPT_RECV_T)) THEN (JoinAllAppt.NEW_APPT_RECV_T) ELSE ""))) = 0 then setnull() else StringToTime(JoinAllAppt.NEW_APPT_RECV_T[1, 8], "%hh:%nn:%ss"),
		IFF(LEN(TRIM(IFF({{ ref('JoinAll') }}.NEW_APPT_RECV_T IS NOT NULL, {{ ref('JoinAll') }}.NEW_APPT_RECV_T, ''))) = 0, SETNULL(), STRINGTOTIME(SUBSTRING({{ ref('JoinAll') }}.NEW_APPT_RECV_T, 1, 8), '%hh:%nn:%ss')) AS APPT_RECV_T
	FROM {{ ref('JoinAll') }}
	WHERE {{ ref('JoinAll') }}.change_code = UPDATE
)

SELECT * FROM XfmCheckDeltaAction__OutTgtApptUpdateDS