{{ config(materialized='view', tags=['DltAPPT_FEAT_SHLFrmTMP_APPT_FEAT']) }}

WITH XfmCheckDeltaAction__OutTgtApptFeatUpdateDS2 AS (
	SELECT
		-- *SRC*: DateFromDaysSince(-1, StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')) AS ExpiryDate,
		1 AS INSERT,
		3 AS UPDATE,
		0 AS COPY,
		-- *SRC*: ( IF IsNotNull((JoinAllApptFeat.NEW_APPT_I)) THEN (JoinAllApptFeat.NEW_APPT_I) ELSE ""),
		IFF({{ ref('JoinAll') }}.NEW_APPT_I IS NOT NULL, {{ ref('JoinAll') }}.NEW_APPT_I, '') AS APPT_I,
		-- *SRC*: ( IF IsNotNull((JoinAllApptFeat.OLD_EFFT_D)) THEN (JoinAllApptFeat.OLD_EFFT_D) ELSE ""),
		IFF({{ ref('JoinAll') }}.OLD_EFFT_D IS NOT NULL, {{ ref('JoinAll') }}.OLD_EFFT_D, '') AS EFFT_D,
		{{ ref('JoinAll') }}.NEW_FEAT_I AS FEAT_I,
		'NOT APPLICABLE' AS SRCE_SYST_APPT_FEAT_I,
		ExpiryDate AS EXPY_D,
		REFR_PK AS PROS_KEY_EXPY_I
	FROM {{ ref('JoinAll') }}
	WHERE {{ ref('JoinAll') }}.change_code = UPDATE OR {{ ref('JoinAll') }}.change_code = COPY AND UPPER({{ ref('JoinAll') }}.NEW_CASS_WITHHOLD_RISKBANK_FLAG) = 'N' OR LEN(TRIM(IFF({{ ref('JoinAll') }}.NEW_CASS_WITHHOLD_RISKBANK_FLAG IS NOT NULL, {{ ref('JoinAll') }}.NEW_CASS_WITHHOLD_RISKBANK_FLAG, ''))) = 0
)

SELECT * FROM XfmCheckDeltaAction__OutTgtApptFeatUpdateDS2