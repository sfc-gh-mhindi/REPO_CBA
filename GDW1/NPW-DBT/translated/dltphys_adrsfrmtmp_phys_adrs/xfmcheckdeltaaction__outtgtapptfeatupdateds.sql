{{ config(materialized='view', tags=['DltPHYS_ADRSFrmTMP_PHYS_ADRS']) }}

WITH XfmCheckDeltaAction__OutTgtApptFeatUpdateDS AS (
	SELECT
		-- *SRC*: DateFromDaysSince(-1, StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')) AS ExpiryDate,
		1 AS INSERT,
		3 AS UPDATE,
		-- *SRC*: ( IF IsNotNull((JoinAllApptFeat.NEW_ADRS_I)) THEN (JoinAllApptFeat.NEW_ADRS_I) ELSE ""),
		IFF({{ ref('JoinAll') }}.NEW_ADRS_I IS NOT NULL, {{ ref('JoinAll') }}.NEW_ADRS_I, '') AS ADRS_I,
		-- *SRC*: ( IF IsNotNull((JoinAllApptFeat.OLD_EFFT_D)) THEN (JoinAllApptFeat.OLD_EFFT_D) ELSE ""),
		IFF({{ ref('JoinAll') }}.OLD_EFFT_D IS NOT NULL, {{ ref('JoinAll') }}.OLD_EFFT_D, '') AS EFFT_D,
		ExpiryDate AS EXPY_D,
		-- *SRC*: AsInteger(REFR_PK),
		ASINTEGER(REFR_PK) AS PROS_KEY_EXPY_I
	FROM {{ ref('JoinAll') }}
	WHERE {{ ref('JoinAll') }}.change_code = UPDATE
)

SELECT * FROM XfmCheckDeltaAction__OutTgtApptFeatUpdateDS