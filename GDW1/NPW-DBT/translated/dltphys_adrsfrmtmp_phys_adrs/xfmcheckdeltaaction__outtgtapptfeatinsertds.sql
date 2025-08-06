{{ config(materialized='view', tags=['DltPHYS_ADRSFrmTMP_PHYS_ADRS']) }}

WITH XfmCheckDeltaAction__OutTgtApptFeatInsertDS AS (
	SELECT
		-- *SRC*: DateFromDaysSince(-1, StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')) AS ExpiryDate,
		1 AS INSERT,
		3 AS UPDATE,
		-- *SRC*: ( IF IsNotNull((JoinAllApptFeat.NEW_ADRS_I)) THEN (JoinAllApptFeat.NEW_ADRS_I) ELSE ""),
		IFF({{ ref('JoinAll') }}.NEW_ADRS_I IS NOT NULL, {{ ref('JoinAll') }}.NEW_ADRS_I, '') AS ADRS_I,
		-- *SRC*: ( IF IsNotNull((JoinAllApptFeat.NEW_PHYS_ADRS_TYPE_C)) THEN (JoinAllApptFeat.NEW_PHYS_ADRS_TYPE_C) ELSE ""),
		IFF({{ ref('JoinAll') }}.NEW_PHYS_ADRS_TYPE_C IS NOT NULL, {{ ref('JoinAll') }}.NEW_PHYS_ADRS_TYPE_C, '') AS PHYS_ADRS_TYPE_C,
		-- *SRC*: ( IF IsNotNull((JoinAllApptFeat.NEW_ADRS_LINE_1_X)) THEN (JoinAllApptFeat.NEW_ADRS_LINE_1_X) ELSE ""),
		IFF({{ ref('JoinAll') }}.NEW_ADRS_LINE_1_X IS NOT NULL, {{ ref('JoinAll') }}.NEW_ADRS_LINE_1_X, '') AS ADRS_LINE_1_X,
		-- *SRC*: ( IF IsNotNull((JoinAllApptFeat.NEW_ADRS_LINE_2_X)) THEN (JoinAllApptFeat.NEW_ADRS_LINE_2_X) ELSE ""),
		IFF({{ ref('JoinAll') }}.NEW_ADRS_LINE_2_X IS NOT NULL, {{ ref('JoinAll') }}.NEW_ADRS_LINE_2_X, '') AS ADRS_LINE_2_X,
		-- *SRC*: ( IF IsNotNull((JoinAllApptFeat.NEW_SURB_X)) THEN (JoinAllApptFeat.NEW_SURB_X) ELSE ""),
		IFF({{ ref('JoinAll') }}.NEW_SURB_X IS NOT NULL, {{ ref('JoinAll') }}.NEW_SURB_X, '') AS SURB_X,
		{{ ref('JoinAll') }}.NEW_CITY_X AS CITY_X,
		{{ ref('JoinAll') }}.NEW_PCOD_C AS PCOD_C,
		{{ ref('JoinAll') }}.NEW_STAT_C AS STAT_C,
		-- *SRC*: ( IF IsNotNull((JoinAllApptFeat.NEW_ISO_CNTY_C)) THEN (JoinAllApptFeat.NEW_ISO_CNTY_C) ELSE ""),
		IFF({{ ref('JoinAll') }}.NEW_ISO_CNTY_C IS NOT NULL, {{ ref('JoinAll') }}.NEW_ISO_CNTY_C, '') AS ISO_CNTY_C,
		'CSE' AS SRCE_SYST_C,
		-- *SRC*: StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd'),
		STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd') AS EFFT_D,
		-- *SRC*: StringToDate('9999-12-31', '%yyyy-%mm-%dd'),
		STRINGTODATE('9999-12-31', '%yyyy-%mm-%dd') AS EXPY_D,
		-- *SRC*: AsInteger(REFR_PK),
		ASINTEGER(REFR_PK) AS PROS_KEY_EFFT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS PROS_KEY_EXPY_I,
		0 AS ROW_SECU_ACCS_C
	FROM {{ ref('JoinAll') }}
	WHERE {{ ref('JoinAll') }}.change_code = INSERT OR {{ ref('JoinAll') }}.change_code = UPDATE
)

SELECT * FROM XfmCheckDeltaAction__OutTgtApptFeatInsertDS