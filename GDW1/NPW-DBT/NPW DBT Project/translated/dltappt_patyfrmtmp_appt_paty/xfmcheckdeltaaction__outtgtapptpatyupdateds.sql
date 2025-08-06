{{ config(materialized='view', tags=['DltAPPT_PATYFrmTMP_APPT_PATY']) }}

WITH XfmCheckDeltaAction__OutTgtApptPatyUpdateDS AS (
	SELECT
		-- *SRC*: DateFromDaysSince(-1, StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')) AS ExpiryDate,
		1 AS INSERT,
		3 AS UPDATE,
		{{ ref('Join') }}.NEW_APPT_I AS APPT_I,
		{{ ref('Join') }}.NEW_PATY_I AS PATY_I,
		-- *SRC*: ( IF IsNotNull((OutJoin.OLD_EFFT_D)) THEN (OutJoin.OLD_EFFT_D) ELSE ""),
		IFF({{ ref('Join') }}.OLD_EFFT_D IS NOT NULL, {{ ref('Join') }}.OLD_EFFT_D, '') AS EFFT_D,
		ExpiryDate AS EXPY_D,
		REFR_PK AS PROS_KEY_EXPY_I
	FROM {{ ref('Join') }}
	WHERE {{ ref('Join') }}.change_code = UPDATE
)

SELECT * FROM XfmCheckDeltaAction__OutTgtApptPatyUpdateDS