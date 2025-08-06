{{ config(materialized='view', tags=['DltEVNT_EMPLFrmTMP_FA_ENV_EVNT']) }}

WITH Join_82 AS (
	SELECT
		{{ ref('CpyTmpFAEnvEvntTera') }}.EVNT_I,
		{{ ref('CpyTmpFAEnvEvntTera') }}.ROW_SECU_ACCS_C,
		{{ ref('CpyTmpFAEnvEvntTera') }}.EMPL_I,
		{{ ref('CpyTmpFAEnvEvntTera') }}.EVNT_PATY_ROLE_TYPE_C,
		{{ ref('CpyTmpFAEnvEvntTera') }}.OLD_EVNT_I,
		{{ ref('CpyTmpFAEnvEvntTera') }}.OLD_EMPL_I,
		{{ ref('CpyTmpFAEnvEvntTera') }}.OLD_EFFT_D,
		{{ ref('ChangeCapture') }}.change_code
	FROM {{ ref('CpyTmpFAEnvEvntTera') }}
	INNER JOIN {{ ref('ChangeCapture') }} ON {{ ref('CpyTmpFAEnvEvntTera') }}.EVNT_I = {{ ref('ChangeCapture') }}.EVNT_I
)

SELECT * FROM Join_82