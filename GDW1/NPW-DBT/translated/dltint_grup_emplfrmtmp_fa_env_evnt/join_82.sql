{{ config(materialized='view', tags=['DltINT_GRUP_EMPLFrmTMP_FA_ENV_EVNT']) }}

WITH Join_82 AS (
	SELECT
		{{ ref('CpyTmpFAEnvEvntTera') }}.FA_ENV_EVNT_ID,
		{{ ref('CpyTmpFAEnvEvntTera') }}.FA_UTAK_ID,
		{{ ref('CpyTmpFAEnvEvntTera') }}.FA_ENV_EVNT_CAT_ID,
		{{ ref('CpyTmpFAEnvEvntTera') }}.CRAT_DATE,
		{{ ref('CpyTmpFAEnvEvntTera') }}.CRAT_BY_STAF_NUM,
		{{ ref('CpyTmpFAEnvEvntTera') }}.COIN_REQ_ID,
		{{ ref('CpyTmpFAEnvEvntTera') }}.ORIG_ETL_D,
		{{ ref('CpyTmpFAEnvEvntTera') }}.INT_GRUP_I,
		{{ ref('CpyTmpFAEnvEvntTera') }}.EMPL_I,
		{{ ref('CpyTmpFAEnvEvntTera') }}.EMPL_ROLE_C,
		{{ ref('CpyTmpFAEnvEvntTera') }}.OLD_INT_GRUP_I,
		{{ ref('CpyTmpFAEnvEvntTera') }}.OLD_EMPL_I,
		{{ ref('CpyTmpFAEnvEvntTera') }}.OLD_EFFT_D,
		{{ ref('ChangeCapture') }}.change_code
	FROM {{ ref('CpyTmpFAEnvEvntTera') }}
	INNER JOIN {{ ref('ChangeCapture') }} ON {{ ref('CpyTmpFAEnvEvntTera') }}.INT_GRUP_I = {{ ref('ChangeCapture') }}.INT_GRUP_I
)

SELECT * FROM Join_82