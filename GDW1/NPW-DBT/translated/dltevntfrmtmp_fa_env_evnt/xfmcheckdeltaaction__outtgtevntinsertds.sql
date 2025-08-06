{{ config(materialized='view', tags=['DltEVNTFrmTMP_FA_ENV_EVNT']) }}

WITH XfmCheckDeltaAction__OutTgtEvntInsertDS AS (
	SELECT
		-- *SRC*: DateFromDaysSince(-1, StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')) AS ExpiryDate,
		1 AS INSERT,
		3 AS UPDATE,
		'N' AS UpdateFlag,
		EVNT_I,
		EVNT_ACTV_TYPE_C,
		BUSN_EVNT_F,
		CTCT_EVNT_F,
		INVT_EVNT_F,
		FNCL_ACCT_EVNT_F,
		FNCL_NVAL_EVNT_F,
		INCD_F,
		INSR_EVNT_F,
		INSR_NVAL_EVNT_F,
		ROW_SECU_ACCS_C,
		REFR_PK AS PROS_KEY_EFFT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS EROR_SEQN_I
	FROM {{ ref('Join_82') }}
	WHERE {{ ref('Join_82') }}.change_code = INSERT
)

SELECT * FROM XfmCheckDeltaAction__OutTgtEvntInsertDS