{{ config(materialized='view', tags=['DltEVNT_EMPLFrmTMP_FA_ENV_EVNT']) }}

WITH CpyTmpFAEnvEvntTera AS (
	SELECT
		EVNT_I,
		EMPL_I,
		{{ ref('SrcTmpFAEnvEvntTera') }}.OLD_EVNT_I AS EVNT_I,
		{{ ref('SrcTmpFAEnvEvntTera') }}.OLD_EMPL_I AS EMPL_I,
		ROW_SECU_ACCS_C,
		EVNT_PATY_ROLE_TYPE_C,
		OLD_EVNT_I,
		OLD_EMPL_I,
		OLD_EFFT_D
	FROM {{ ref('SrcTmpFAEnvEvntTera') }}
)

SELECT * FROM CpyTmpFAEnvEvntTera