{{ config(materialized='view', tags=['DltINT_GRUP_EMPLFrmTMP_FA_ENV_EVNT']) }}

WITH CpyTmpFAEnvEvntTera AS (
	SELECT
		INT_GRUP_I,
		EMPL_I,
		{{ ref('SrcTmpFAEnvEvntTera') }}.OLD_INT_GRUP_I AS INT_GRUP_I,
		{{ ref('SrcTmpFAEnvEvntTera') }}.OLD_EMPL_I AS EMPL_I,
		FA_ENV_EVNT_ID,
		FA_UTAK_ID,
		FA_ENV_EVNT_CAT_ID,
		CRAT_DATE,
		CRAT_BY_STAF_NUM,
		COIN_REQ_ID,
		ORIG_ETL_D,
		EMPL_ROLE_C,
		OLD_INT_GRUP_I,
		OLD_EMPL_I,
		OLD_EFFT_D
	FROM {{ ref('SrcTmpFAEnvEvntTera') }}
)

SELECT * FROM CpyTmpFAEnvEvntTera