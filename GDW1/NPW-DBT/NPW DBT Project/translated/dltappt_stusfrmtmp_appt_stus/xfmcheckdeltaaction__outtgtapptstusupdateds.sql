{{ config(materialized='view', tags=['DltAPPT_STUSFrmTMP_APPT_STUS']) }}

WITH XfmCheckDeltaAction__OutTgtApptStusUpdateDS AS (
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
		-- *SRC*: ( IF IsNotNull((JoinAllApptStus.OLD_EFFT_D)) THEN (JoinAllApptStus.OLD_EFFT_D) ELSE ""),
		IFF({{ ref('JoinAll') }}.OLD_EFFT_D IS NOT NULL, {{ ref('JoinAll') }}.OLD_EFFT_D, '') AS EFFT_D,
		ExpiryDate AS EXPY_D,
		REFR_PK AS PROS_KEY_EXPY_I
	FROM {{ ref('JoinAll') }}
	WHERE {{ ref('JoinAll') }}.change_code = UPDATE
)

SELECT * FROM XfmCheckDeltaAction__OutTgtApptStusUpdateDS