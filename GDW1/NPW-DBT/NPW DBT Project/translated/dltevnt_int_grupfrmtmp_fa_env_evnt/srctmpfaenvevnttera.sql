{{ config(materialized='view', tags=['DltEVNT_INT_GRUPFrmTMP_FA_ENV_EVNT']) }}

WITH 
tmp_fa_env_evnt AS (
	SELECT
	*
	FROM {{ ref("tmp_fa_env_evnt")  }}),
evnt_int_grup AS (
	SELECT
	*
	FROM {{ ref("evnt_int_grup")  }}),
SrcTmpFAEnvEvntTera AS (SELECT a.INT_GRUP_I, a.EVNT_I, b.INT_GRUP_I AS OLD_INT_GRUP_I, b.EVNT_I AS OLD_EVNT_I FROM TMP_FA_ENV_EVNT LEFT OUTER JOIN EVNT_INT_GRUP ON TRIM(a.INT_GRUP_I) = TRIM(b.INT_GRUP_I) AND TRIM(a.EVNT_I) = TRIM(b.EVNT_I) AND b.EXPY_D = '9999-12-31')


SELECT * FROM SrcTmpFAEnvEvntTera