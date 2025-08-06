{{ config(materialized='view', tags=['DltEVNTFrmTMP_FA_ENV_EVNT']) }}

WITH Join_82 AS (
	SELECT
		{{ ref('CpyTmpFAEnvEvntTera') }}.EVNT_I,
		{{ ref('CpyTmpFAEnvEvntTera') }}.EVNT_ACTV_TYPE_C,
		{{ ref('CpyTmpFAEnvEvntTera') }}.BUSN_EVNT_F,
		{{ ref('CpyTmpFAEnvEvntTera') }}.CTCT_EVNT_F,
		{{ ref('CpyTmpFAEnvEvntTera') }}.INVT_EVNT_F,
		{{ ref('CpyTmpFAEnvEvntTera') }}.FNCL_ACCT_EVNT_F,
		{{ ref('CpyTmpFAEnvEvntTera') }}.FNCL_NVAL_EVNT_F,
		{{ ref('CpyTmpFAEnvEvntTera') }}.INCD_F,
		{{ ref('CpyTmpFAEnvEvntTera') }}.INSR_EVNT_F,
		{{ ref('CpyTmpFAEnvEvntTera') }}.INSR_NVAL_EVNT_F,
		{{ ref('CpyTmpFAEnvEvntTera') }}.ROW_SECU_ACCS_C,
		{{ ref('ChangeCapture') }}.change_code
	FROM {{ ref('CpyTmpFAEnvEvntTera') }}
	INNER JOIN {{ ref('ChangeCapture') }} ON {{ ref('CpyTmpFAEnvEvntTera') }}.EVNT_I = {{ ref('ChangeCapture') }}.EVNT_I
)

SELECT * FROM Join_82