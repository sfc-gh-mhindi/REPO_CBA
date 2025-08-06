{{ config(materialized='view', tags=['DltINT_GRUP_EMPLFrmTMP_FA_ENV_EVNT']) }}

WITH 
tmp_fa_env_evnt AS (
	SELECT
	*
	FROM {{ ref("tmp_fa_env_evnt")  }}),
int_grup_empl AS (
	SELECT
	*
	FROM {{ ref("int_grup_empl")  }}),
SrcTmpFAEnvEvntTera AS (SELECT a.FA_ENV_EVNT_ID, a.FA_UTAK_ID, a.FA_ENV_EVNT_CAT_ID, a.CRAT_DATE, a.CRAT_BY_STAF_NUM, a.COIN_REQ_ID, a.ORIG_ETL_D, a.INT_GRUP_I, a.EMPL_I, a.EMPL_ROLE_C, b.INT_GRUP_I AS OLD_INT_GRUP_I, b.EMPL_I AS OLD_EMPL_I, b.EFFT_D AS OLD_EFFT_D FROM TMP_FA_ENV_EVNT LEFT OUTER JOIN INT_GRUP_EMPL ON TRIM(a.INT_GRUP_I) = TRIM(b.INT_GRUP_I) AND b.EXPY_D = '9999-12-31' WHERE TRIM(a.FA_ENV_EVNT_CAT_ID) = '1' AND a.INT_GRUP_EMPL_F = 'Y')


SELECT * FROM SrcTmpFAEnvEvntTera