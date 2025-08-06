{{ config(materialized='view', tags=['DltAPPT_TRNF_DETLFrmTMP_APPT_TRNF_DETL']) }}

WITH XfmCheckDeltaAction__OutTgtApptTrnfDetlUpdateDS AS (
	SELECT
		-- *SRC*: DateFromDaysSince(-1, StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')) AS ExpiryDate,
		1 AS INSERT,
		3 AS UPDATE,
		-- *SRC*: ( IF IsNotNull((OutJoin.NEW_APPT_I)) THEN (OutJoin.NEW_APPT_I) ELSE ""),
		IFF({{ ref('Join') }}.NEW_APPT_I IS NOT NULL, {{ ref('Join') }}.NEW_APPT_I, '') AS APPT_I,
		-- *SRC*: ( IF IsNotNull((OutJoin.NEW_APPT_TRNF_I)) THEN (OutJoin.NEW_APPT_TRNF_I) ELSE ""),
		IFF({{ ref('Join') }}.NEW_APPT_TRNF_I IS NOT NULL, {{ ref('Join') }}.NEW_APPT_TRNF_I, '') AS APPT_TRNF_I,
		-- *SRC*: ( IF IsNotNull((OutJoin.OLD_EFFT_D)) THEN (OutJoin.OLD_EFFT_D) ELSE ""),
		IFF({{ ref('Join') }}.OLD_EFFT_D IS NOT NULL, {{ ref('Join') }}.OLD_EFFT_D, '') AS EFFT_D,
		ExpiryDate AS EXPY_D,
		REFR_PK AS PROS_KEY_EXPY_I
	FROM {{ ref('Join') }}
	WHERE {{ ref('Join') }}.change_code = UPDATE
)

SELECT * FROM XfmCheckDeltaAction__OutTgtApptTrnfDetlUpdateDS