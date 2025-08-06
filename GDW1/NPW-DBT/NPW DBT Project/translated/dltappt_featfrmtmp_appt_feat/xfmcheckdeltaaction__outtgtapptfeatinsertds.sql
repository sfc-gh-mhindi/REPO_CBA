{{ config(materialized='view', tags=['DltAPPT_FEATFrmTMP_APPT_FEAT']) }}

WITH XfmCheckDeltaAction__OutTgtApptFeatInsertDS AS (
	SELECT
		-- *SRC*: DateFromDaysSince(-1, StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')) AS ExpiryDate,
		1 AS INSERT,
		3 AS UPDATE,
		-- *SRC*: ( IF IsNotNull((JoinAllApptFeat.NEW_APPT_I)) THEN (JoinAllApptFeat.NEW_APPT_I) ELSE ""),
		IFF({{ ref('JoinAll') }}.NEW_APPT_I IS NOT NULL, {{ ref('JoinAll') }}.NEW_APPT_I, '') AS APPT_I,
		{{ ref('JoinAll') }}.NEW_FEAT_I AS FEAT_I,
		-- *SRC*: StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd'),
		STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd') AS EFFT_D,
		{{ ref('JoinAll') }}.NEW_SRCE_SYST_C AS SRCE_SYST_C,
		{{ ref('JoinAll') }}.NEW_SRCE_SYST_APPT_FEAT_I AS SRCE_SYST_APPT_FEAT_I,
		{{ ref('JoinAll') }}.NEW_SRCE_SYST_STND_VALU_Q AS SRCE_SYST_STND_VALU_Q,
		{{ ref('JoinAll') }}.NEW_SRCE_SYST_STND_VALU_R AS SRCE_SYST_STND_VALU_R,
		{{ ref('JoinAll') }}.NEW_SRCE_SYST_STND_VALU_A AS SRCE_SYST_STND_VALU_A,
		{{ ref('JoinAll') }}.NEW_ACTL_VALU_Q AS ACTL_VALU_Q,
		{{ ref('JoinAll') }}.NEW_ACTL_VALU_R AS ACTL_VALU_R,
		{{ ref('JoinAll') }}.NEW_ACTL_VALU_A AS ACTL_VALU_A,
		{{ ref('JoinAll') }}.NEW_CNCY_C AS CNCY_C,
		{{ ref('JoinAll') }}.NEW_OVRD_FEAT_I AS OVRD_FEAT_I,
		{{ ref('JoinAll') }}.NEW_OVRD_REAS_C AS OVRD_REAS_C,
		{{ ref('JoinAll') }}.NEW_FEAT_SEQN_N AS FEAT_SEQN_N,
		{{ ref('JoinAll') }}.NEW_FEAT_STRT_D AS FEAT_STRT_D,
		{{ ref('JoinAll') }}.NEW_FEE_CHRG_D AS FEE_CHRG_D,
		-- *SRC*: StringToDate('9999-12-31', '%yyyy-%mm-%dd'),
		STRINGTODATE('9999-12-31', '%yyyy-%mm-%dd') AS EXPY_D,
		REFR_PK AS PROS_KEY_EFFT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS PROS_KEY_EXPY_I,
		-- *SRC*: SetNull(),
		SETNULL() AS EROR_SEQN_I
	FROM {{ ref('JoinAll') }}
	WHERE {{ ref('JoinAll') }}.change_code = INSERT OR {{ ref('JoinAll') }}.change_code = UPDATE
)

SELECT * FROM XfmCheckDeltaAction__OutTgtApptFeatInsertDS