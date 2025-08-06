{{ config(materialized='view', tags=['DltEVNTFrmTMP_FA_ENV_EVNT']) }}

WITH 
tmp_fa_env_evnt AS (
	SELECT
	*
	FROM {{ ref("tmp_fa_env_evnt")  }}),
evnt AS (
	SELECT
	*
	FROM {{ ref("evnt")  }}),
SrcTmpFAEnvEvntTera AS (SELECT a.EVNT_I, a.EVNT_ACTV_TYPE_C, a.BUSN_EVNT_F, a.CTCT_EVNT_F, a.INVT_EVNT_F, a.FNCL_ACCT_EVNT_F, a.FNCL_NVAL_EVNT_F, a.INCD_F, a.INSR_EVNT_F, a.INSR_NVAL_EVNT_F, a.ROW_SECU_ACCS_C, b.EVNT_I AS OLD_EVNT_I FROM TMP_FA_ENV_EVNT LEFT OUTER JOIN EVNT ON TRIM(a.EVNT_I) = b.EVNT_I)


SELECT * FROM SrcTmpFAEnvEvntTera