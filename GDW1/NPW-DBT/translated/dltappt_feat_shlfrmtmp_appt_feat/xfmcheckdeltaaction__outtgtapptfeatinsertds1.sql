{{ config(materialized='view', tags=['DltAPPT_FEAT_SHLFrmTMP_APPT_FEAT']) }}

WITH XfmCheckDeltaAction__OutTgtApptFeatInsertDS1 AS (
	SELECT
		-- *SRC*: DateFromDaysSince(-1, StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')) AS ExpiryDate,
		1 AS INSERT,
		3 AS UPDATE,
		0 AS COPY,
		-- *SRC*: ( IF IsNotNull((JoinAllApptFeat.NEW_APPT_I)) THEN (JoinAllApptFeat.NEW_APPT_I) ELSE ""),
		IFF({{ ref('JoinAll') }}.NEW_APPT_I IS NOT NULL, {{ ref('JoinAll') }}.NEW_APPT_I, '') AS APPT_I,
		-- *SRC*: ( IF IsNotNull((JoinAllApptFeat.NEW_FEAT_I)) THEN (JoinAllApptFeat.NEW_FEAT_I) ELSE ""),
		IFF({{ ref('JoinAll') }}.NEW_FEAT_I IS NOT NULL, {{ ref('JoinAll') }}.NEW_FEAT_I, '') AS FEAT_I,
		-- *SRC*: StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd'),
		STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd') AS EFFT_D,
		{{ ref('JoinAll') }}.NEW_SRCE_SYST_C AS SRCE_SYST_C,
		'NOT APPLICABLE' AS SRCE_SYST_APPT_FEAT_I,
		-- *SRC*: setnull(),
		SETNULL() AS SRCE_SYST_STND_VALU_Q,
		-- *SRC*: setnull(),
		SETNULL() AS SRCE_SYST_STND_VALU_R,
		-- *SRC*: setnull(),
		SETNULL() AS SRCE_SYST_STND_VALU_A,
		-- *SRC*: setnull(),
		SETNULL() AS ACTL_VALU_Q,
		-- *SRC*: setnull(),
		SETNULL() AS ACTL_VALU_R,
		-- *SRC*: setnull(),
		SETNULL() AS ACTL_VALU_A,
		-- *SRC*: setnull(),
		SETNULL() AS CNCY_C,
		-- *SRC*: setnull(),
		SETNULL() AS OVRD_FEAT_I,
		-- *SRC*: setnull(),
		SETNULL() AS OVRD_REAS_C,
		-- *SRC*: setnull(),
		SETNULL() AS FEAT_SEQN_N,
		-- *SRC*: setnull(),
		SETNULL() AS FEAT_STRT_D,
		-- *SRC*: setnull(),
		SETNULL() AS FEE_CHRG_D,
		-- *SRC*: StringToDate('9999-12-31', '%yyyy-%mm-%dd'),
		STRINGTODATE('9999-12-31', '%yyyy-%mm-%dd') AS EXPY_D,
		REFR_PK AS PROS_KEY_EFFT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS PROS_KEY_EXPY_I,
		-- *SRC*: SetNull(),
		SETNULL() AS EROR_SEQN_I
	FROM {{ ref('JoinAll') }}
	WHERE {{ ref('JoinAll') }}.change_code = INSERT AND TRIM({{ ref('JoinAll') }}.NEW_SHL_F) = 'Y' AND {{ ref('JoinAll') }}.NEW_CASS_WITHHOLD_RISKBANK_FLAG = 'R'
)

SELECT * FROM XfmCheckDeltaAction__OutTgtApptFeatInsertDS1