{{ config(materialized='view', tags=['DltBUSN_EVNTFrmTMP_FA_ENV_EVNT']) }}

WITH Join_82 AS (
	SELECT
		{{ ref('CpyTmpFAEnvEvntTera') }}.EVNT_I,
		{{ ref('CpyTmpFAEnvEvntTera') }}.ROW_SECU_ACCS_C,
		{{ ref('CpyTmpFAEnvEvntTera') }}.SRCE_SYST_EVNT_I,
		{{ ref('CpyTmpFAEnvEvntTera') }}.SRCE_SYST_EVNT_TYPE_I,
		{{ ref('CpyTmpFAEnvEvntTera') }}.EVNT_ACTL_D,
		{{ ref('CpyTmpFAEnvEvntTera') }}.EVNT_ACTL_T,
		{{ ref('CpyTmpFAEnvEvntTera') }}.SRCE_SYST_C,
		{{ ref('ChangeCapture') }}.change_code
	FROM {{ ref('CpyTmpFAEnvEvntTera') }}
	INNER JOIN {{ ref('ChangeCapture') }} ON {{ ref('CpyTmpFAEnvEvntTera') }}.EVNT_I = {{ ref('ChangeCapture') }}.EVNT_I
)

SELECT * FROM Join_82