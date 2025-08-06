{{ config(materialized='view', tags=['DltEVNT_INT_GRUPFrmTMP_FA_ENV_EVNT']) }}

WITH CpyTmpFAEnvEvntTera AS (
	SELECT
		INT_GRUP_I,
		EVNT_I,
		{{ ref('SrcTmpFAEnvEvntTera') }}.OLD_INT_GRUP_I AS INT_GRUP_I,
		{{ ref('SrcTmpFAEnvEvntTera') }}.OLD_EVNT_I AS EVNT_I
	FROM {{ ref('SrcTmpFAEnvEvntTera') }}
)

SELECT * FROM CpyTmpFAEnvEvntTera