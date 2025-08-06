{{ config(materialized='view', tags=['DltBUSN_EVNTFrmTMP_FA_ENV_EVNT']) }}

WITH CpyTmpFAEnvEvntTera AS (
	SELECT
		EVNT_I,
		{{ ref('SrcTmpFAEnvEvntTera') }}.OLD_EVNT_I AS EVNT_I,
		ROW_SECU_ACCS_C,
		SRCE_SYST_EVNT_I,
		SRCE_SYST_EVNT_TYPE_I,
		EVNT_ACTL_D,
		EVNT_ACTL_T,
		SRCE_SYST_C
	FROM {{ ref('SrcTmpFAEnvEvntTera') }}
)

SELECT * FROM CpyTmpFAEnvEvntTera