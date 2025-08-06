{{ config(materialized='view', tags=['DltEVNT_EMPLFrmTMP_FA_ENV_EVNT']) }}

WITH 
tmp_fa_env_evnt AS (
	SELECT
	*
	FROM {{ ref("tmp_fa_env_evnt")  }}),
evnt_empl AS (
	SELECT
	*
	FROM {{ ref("evnt_empl")  }}),
SrcTmpFAEnvEvntTera AS (SELECT a.EVNT_I, a.ROW_SECU_ACCS_C, a.EMPL_I, a.EVNT_PATY_ROLE_TYPE_C, b.EVNT_I AS OLD_EVNT_I, b.EMPL_I AS OLD_EMPL_I, b.EFFT_D AS OLD_EFFT_D FROM TMP_FA_ENV_EVNT LEFT OUTER JOIN EVNT_EMPL ON a.EVNT_I = b.EVNT_I AND b.EXPY_D = '9999-12-31' WHERE a.EVNT_EMPL_F = 'Y')


SELECT * FROM SrcTmpFAEnvEvntTera