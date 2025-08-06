{{ config(materialized='view', tags=['DltAPPT_PDCT_FEAT_FrmTMP_APPT_PDCT_FEAT_PEXAHM_ONEOFF']) }}

WITH XfmCheckDeltaAction__OutTgtApptPdctFeatUpdate AS (
	SELECT
		-- *SRC*: DateFromDaysSince(-1, StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')) AS ExpiryDate,
		-- *SRC*: ( IF IsNotNull((InTmpApptPdctFeatTera.NEW_APPT_PDCT_I)) THEN (InTmpApptPdctFeatTera.NEW_APPT_PDCT_I) ELSE ""),
		IFF({{ ref('SrcTmpApptPDctFeatTera') }}.NEW_APPT_PDCT_I IS NOT NULL, {{ ref('SrcTmpApptPDctFeatTera') }}.NEW_APPT_PDCT_I, '') AS APPT_PDCT_I,
		{{ ref('SrcTmpApptPDctFeatTera') }}.NEW_FEAT_I AS FEAT_I,
		{{ ref('SrcTmpApptPDctFeatTera') }}.NEW_SRCE_SYST_C AS SRCE_SYST_C,
		{{ ref('SrcTmpApptPDctFeatTera') }}.OLD_EFFT_D AS EFFT_D,
		ExpiryDate AS EXPY_D,
		REFR_PK AS PROS_KEY_EXPY_I
	FROM {{ ref('SrcTmpApptPDctFeatTera') }}
	WHERE {{ ref('SrcTmpApptPDctFeatTera') }}.REC_TYPE_I = 'U'
)

SELECT * FROM XfmCheckDeltaAction__OutTgtApptPdctFeatUpdate