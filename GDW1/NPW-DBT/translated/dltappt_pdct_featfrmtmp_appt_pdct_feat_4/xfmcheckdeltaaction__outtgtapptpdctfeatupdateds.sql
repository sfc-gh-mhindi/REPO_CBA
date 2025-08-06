{{ config(materialized='view', tags=['DltAPPT_PDCT_FEATFrmTMP_APPT_PDCT_FEAT_4']) }}

WITH XfmCheckDeltaAction__OutTgtApptPdctFeatUpdateDS AS (
	SELECT
		-- *SRC*: DateFromDaysSince(-1, StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')) AS ExpiryDate,
		1 AS INSERT,
		3 AS UPDATE,
		-- *SRC*: ( IF IsNotNull((JoinAllApptPdctFeat.NEW_APPT_PDCT_I)) THEN (JoinAllApptPdctFeat.NEW_APPT_PDCT_I) ELSE ""),
		IFF({{ ref('JoinAll') }}.NEW_APPT_PDCT_I IS NOT NULL, {{ ref('JoinAll') }}.NEW_APPT_PDCT_I, '') AS APPT_PDCT_I,
		-- *SRC*: ( IF IsNotNull((JoinAllApptPdctFeat.OLD_FEAT_I)) THEN (JoinAllApptPdctFeat.OLD_FEAT_I) ELSE ""),
		IFF({{ ref('JoinAll') }}.OLD_FEAT_I IS NOT NULL, {{ ref('JoinAll') }}.OLD_FEAT_I, '') AS FEAT_I,
		-- *SRC*: ( IF IsNotNull((JoinAllApptPdctFeat.OLD_EFFT_D)) THEN (JoinAllApptPdctFeat.OLD_EFFT_D) ELSE ""),
		IFF({{ ref('JoinAll') }}.OLD_EFFT_D IS NOT NULL, {{ ref('JoinAll') }}.OLD_EFFT_D, '') AS EFFT_D,
		ExpiryDate AS EXPY_D,
		REFR_PK AS PROS_KEY_EXPY_I
	FROM {{ ref('JoinAll') }}
	WHERE {{ ref('JoinAll') }}.change_code = UPDATE
)

SELECT * FROM XfmCheckDeltaAction__OutTgtApptPdctFeatUpdateDS