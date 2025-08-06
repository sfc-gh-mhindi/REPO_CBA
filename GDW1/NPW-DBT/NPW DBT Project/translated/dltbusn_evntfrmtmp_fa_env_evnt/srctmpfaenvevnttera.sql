{{ config(materialized='view', tags=['DltBUSN_EVNTFrmTMP_FA_ENV_EVNT']) }}

WITH 
busn_evnt AS (
	SELECT
	*
	FROM {{ ref("busn_evnt")  }}),
tmp_fa_env_evnt AS (
	SELECT
	*
	FROM {{ ref("tmp_fa_env_evnt")  }}),
SrcTmpFAEnvEvntTera AS (SELECT a.EVNT_I, a.ROW_SECU_ACCS_C, a.SRCE_SYST_EVNT_I, a.SRCE_SYST_EVNT_TYPE_I, a.EVNT_ACTL_D, CAST(a.EVNT_ACTL_T AS VARCHAR(15)) AS EVNT_ACTL_T, a.SRCE_SYST_C, b.EVNT_I AS OLD_EVNT_I FROM TMP_FA_ENV_EVNT LEFT OUTER JOIN BUSN_EVNT ON TRIM(a.EVNT_I) = b.EVNT_I)


SELECT * FROM SrcTmpFAEnvEvntTera