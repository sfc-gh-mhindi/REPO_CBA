{{ config(materialized='view', tags=['DltAPPT_ASES_DETLFrmTMP_APPT_ASES_DETL']) }}

WITH XfmCheckDeltaAction__OutTgtApptAsesDetlUpdateDS AS (
	SELECT
		-- *SRC*: DateFromDaysSince(-1, StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')) AS ExpiryDate,
		1 AS INSERT,
		3 AS UPDATE,
		-- *SRC*: ( IF IsNotNull((JoinAllApptAsesDetl.NEW_APPT_I)) THEN (JoinAllApptAsesDetl.NEW_APPT_I) ELSE ""),
		IFF({{ ref('JoinAll') }}.NEW_APPT_I IS NOT NULL, {{ ref('JoinAll') }}.NEW_APPT_I, '') AS APPT_I,
		-- *SRC*: ( IF IsNotNull((JoinAllApptAsesDetl.NEW_AMT_TYPE_C)) THEN (JoinAllApptAsesDetl.NEW_AMT_TYPE_C) ELSE ""),
		IFF({{ ref('JoinAll') }}.NEW_AMT_TYPE_C IS NOT NULL, {{ ref('JoinAll') }}.NEW_AMT_TYPE_C, '') AS AMT_TYPE_C,
		-- *SRC*: ( IF IsNotNull((JoinAllApptAsesDetl.OLD_EFFT_D)) THEN (JoinAllApptAsesDetl.OLD_EFFT_D) ELSE ""),
		IFF({{ ref('JoinAll') }}.OLD_EFFT_D IS NOT NULL, {{ ref('JoinAll') }}.OLD_EFFT_D, '') AS EFFT_D,
		ExpiryDate AS EXPY_D,
		REFR_PK AS PROS_KEY_EXPY_I
	FROM {{ ref('JoinAll') }}
	WHERE {{ ref('JoinAll') }}.change_code = UPDATE
)

SELECT * FROM XfmCheckDeltaAction__OutTgtApptAsesDetlUpdateDS