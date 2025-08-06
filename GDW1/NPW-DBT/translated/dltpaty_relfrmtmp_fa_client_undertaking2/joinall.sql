{{ config(materialized='view', tags=['DltPATY_RELFrmTMP_FA_CLIENT_UNDERTAKING2']) }}

WITH JoinAll AS (
	SELECT
		{{ ref('ChangeCapture') }}.REL_I,
		{{ ref('ChangeCapture') }}.REL_TYPE_C,
		{{ ref('ChangeCapture') }}.RELD_PATY_I,
		{{ ref('ChangeCapture') }}.REL_LEVL_C,
		{{ ref('ChangeCapture') }}.change_code,
		{{ ref('ChangeCapture') }}.SRCE_SYST_REL_I,
		{{ ref('ChangeCapture') }}.SRCE_SYST_C,
		{{ ref('ChangeCapture') }}.PATY_ROLE_C,
		{{ ref('ChangeCapture') }}.REL_STUS_C,
		{{ ref('ChangeCapture') }}.REL_EFFT_D,
		{{ ref('ChangeCapture') }}.REL_EXPY_D,
		{{ ref('ChangeCapture') }}.REL_REAS_C,
		{{ ref('CpyFAClientUndertaking') }}.CSE_FA_CLNT_UNTK_ID,
		{{ ref('CpyFAClientUndertaking') }}.CSE_FA_UNTK_ID,
		{{ ref('CpyFAClientUndertaking') }}.CSE_COIN_ENTY_ID,
		{{ ref('CpyFAClientUndertaking') }}.CSE_CLNT_CORR_ID,
		{{ ref('CpyFAClientUndertaking') }}.CSE_FA_ENTY_CAT_ID,
		{{ ref('CpyFAClientUndertaking') }}.CSE_FA_CHLD_STAT_CAT_ID,
		{{ ref('CpyFAClientUndertaking') }}.CSE_CLNT_REL_TYPE_ID,
		{{ ref('CpyFAClientUndertaking') }}.CSE_CLNT_POSN,
		{{ ref('CpyFAClientUndertaking') }}.CSE_IS_PRIM_FLAG,
		{{ ref('CpyFAClientUndertaking') }}.CSE_CIF_CODE,
		{{ ref('CpyFAClientUndertaking') }}.CSE_ORIG_ETL_D,
		{{ ref('ChangeCapture') }}.PATY_I,
		{{ ref('CpyFAClientUndertaking') }}.OLD_REL_I,
		{{ ref('CpyFAClientUndertaking') }}.OLD_RELD_PATY_I,
		{{ ref('CpyFAClientUndertaking') }}.OLD_REL_LEVL_C,
		{{ ref('CpyFAClientUndertaking') }}.OLD_EFFT_D,
		{{ ref('CpyFAClientUndertaking') }}.OLD_PATY_I
	FROM {{ ref('ChangeCapture') }}
	INNER JOIN {{ ref('CpyFAClientUndertaking') }} ON {{ ref('ChangeCapture') }}.REL_I = {{ ref('CpyFAClientUndertaking') }}.REL_I
	AND {{ ref('ChangeCapture') }}.REL_LEVL_C = {{ ref('CpyFAClientUndertaking') }}.REL_LEVL_C
)

SELECT * FROM JoinAll