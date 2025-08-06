{{ config(materialized='view', tags=['DltEVNTFrmTMP_FA_ENV_EVNT']) }}

WITH CpyTmpFAEnvEvntTera AS (
	SELECT
		EVNT_I,
		{{ ref('SrcTmpFAEnvEvntTera') }}.OLD_EVNT_I AS EVNT_I,
		EVNT_ACTV_TYPE_C,
		BUSN_EVNT_F,
		CTCT_EVNT_F,
		INVT_EVNT_F,
		FNCL_ACCT_EVNT_F,
		FNCL_NVAL_EVNT_F,
		INCD_F,
		INSR_EVNT_F,
		INSR_NVAL_EVNT_F,
		ROW_SECU_ACCS_C
	FROM {{ ref('SrcTmpFAEnvEvntTera') }}
)

SELECT * FROM CpyTmpFAEnvEvntTera