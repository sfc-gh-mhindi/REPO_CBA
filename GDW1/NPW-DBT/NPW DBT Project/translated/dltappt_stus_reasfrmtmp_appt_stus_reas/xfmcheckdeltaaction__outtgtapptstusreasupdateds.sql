{{ config(materialized='view', tags=['DltAPPT_STUS_REASFrmTMP_APPT_STUS_REAS']) }}

WITH XfmCheckDeltaAction__OutTgtApptStusReasUpdateDS AS (
	SELECT
		-- *SRC*: DateFromDaysSince(-1, StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')) AS ExpiryDate,
		1 AS INSERT,
		3 AS UPDATE,
		-- *SRC*: ( IF IsNotNull((JoinAllApptStusReas.NEW_APPT_I)) THEN (JoinAllApptStusReas.NEW_APPT_I) ELSE ""),
		IFF({{ ref('JoinAll') }}.NEW_APPT_I IS NOT NULL, {{ ref('JoinAll') }}.NEW_APPT_I, '') AS APPT_I,
		-- *SRC*: ( IF IsNotNull((JoinAllApptStusReas.NEW_STUS_C)) THEN (JoinAllApptStusReas.NEW_STUS_C) ELSE ""),
		IFF({{ ref('JoinAll') }}.NEW_STUS_C IS NOT NULL, {{ ref('JoinAll') }}.NEW_STUS_C, '') AS STUS_C,
		-- *SRC*: ( IF IsNotNull((JoinAllApptStusReas.NEW_STUS_REAS_TYPE_C)) THEN (JoinAllApptStusReas.NEW_STUS_REAS_TYPE_C) ELSE ""),
		IFF({{ ref('JoinAll') }}.NEW_STUS_REAS_TYPE_C IS NOT NULL, {{ ref('JoinAll') }}.NEW_STUS_REAS_TYPE_C, '') AS STUS_REAS_TYPE_C,
		-- *SRC*: ( IF IsNotNull((JoinAllApptStusReas.NEW_STRT_S)) THEN (JoinAllApptStusReas.NEW_STRT_S) ELSE ""),
		IFF({{ ref('JoinAll') }}.NEW_STRT_S IS NOT NULL, {{ ref('JoinAll') }}.NEW_STRT_S, '') AS STRT_S,
		-- *SRC*: ( IF IsNotNull((JoinAllApptStusReas.OLD_EFFT_D)) THEN (JoinAllApptStusReas.OLD_EFFT_D) ELSE ""),
		IFF({{ ref('JoinAll') }}.OLD_EFFT_D IS NOT NULL, {{ ref('JoinAll') }}.OLD_EFFT_D, '') AS EFFT_D,
		ExpiryDate AS EXPY_D,
		REFR_PK AS PROS_KEY_EXPY_I
	FROM {{ ref('JoinAll') }}
	WHERE {{ ref('JoinAll') }}.change_code = UPDATE
)

SELECT * FROM XfmCheckDeltaAction__OutTgtApptStusReasUpdateDS