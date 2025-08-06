{{ config(materialized='view', tags=['DltBUSN_EVNTFrmTMP_FA_ENV_EVNT']) }}

WITH XfmCheckDeltaAction__OutTgtBusnEvntUpdateDS AS (
	SELECT
		-- *SRC*: DateFromDaysSince(-1, StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')) AS ExpiryDate,
		1 AS INSERT,
		3 AS UPDATE,
		'N' AS UpdateFlag,
		-- *SRC*: ( IF IsNotNull((DSLink84.EVNT_I)) THEN (DSLink84.EVNT_I) ELSE ""),
		IFF({{ ref('Join_82') }}.EVNT_I IS NOT NULL, {{ ref('Join_82') }}.EVNT_I, '') AS EVNT_I,
		ROW_SECU_ACCS_C,
		SRCE_SYST_EVNT_I,
		SRCE_SYST_EVNT_TYPE_I,
		EVNT_ACTL_D,
		EVNT_ACTL_T,
		-- *SRC*: ( IF IsNotNull((DSLink84.SRCE_SYST_C)) THEN (DSLink84.SRCE_SYST_C) ELSE ""),
		IFF({{ ref('Join_82') }}.SRCE_SYST_C IS NOT NULL, {{ ref('Join_82') }}.SRCE_SYST_C, '') AS SRCE_SYST_C,
		REFR_PK AS PROS_KEY_EFFT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS EROR_SEQN_I
	FROM {{ ref('Join_82') }}
	WHERE UpdateFlag = 'Y'
)

SELECT * FROM XfmCheckDeltaAction__OutTgtBusnEvntUpdateDS