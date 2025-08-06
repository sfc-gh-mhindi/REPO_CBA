{{ config(materialized='view', tags=['XfmEvnt']) }}

WITH EvntInserts AS (
	SELECT
		EVNT_I,
		EVNT_ACTV_TYPE_C,
		INVT_EVNT_F,
		FNCL_ACCT_EVNT_F,
		CTCT_EVNT_F,
		BUSN_EVNT_F,
		pGDW_PROS_ID AS PROS_KEY_EFFT_I,
		EROR_SEQN_I,
		FNCL_NVAL_EVNT_F,
		INCD_F,
		INSR_EVNT_F,
		INSR_NVAL_EVNT_F,
		ROW_SECU_ACCS_C,
		FNCL_GL_EVNT_F,
		AUTT_AUTN_EVNT_F,
		COLL_EVNT_F
	FROM {{ ref('LeftJoinEvnt') }}
	WHERE {{ ref('LeftJoinEvnt') }}.dummy = 0
)

SELECT * FROM EvntInserts