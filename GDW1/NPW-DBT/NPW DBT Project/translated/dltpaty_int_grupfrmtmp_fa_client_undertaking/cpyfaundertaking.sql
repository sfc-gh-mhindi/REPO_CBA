{{ config(materialized='view', tags=['DltPATY_INT_GRUPFrmTMP_FA_CLIENT_UNDERTAKING']) }}

WITH CpyFAUndertaking AS (
	SELECT
		INT_GRUP_I,
		REL_I,
		SRCE_SYST_PATY_INT_GRUP_I,
		ORIG_SRCE_SYST_PATY_I,
		ORIG_SRCE_SYST_PATY_TYPE_C,
		PRIM_CLNT_F,
		PATY_I,
		SRCE_SYST_C,
		{{ ref('SrcTmpFAClientUndertakingTera') }}.OLD_INT_GRUP_I AS INT_GRUP_I,
		{{ ref('SrcTmpFAClientUndertakingTera') }}.OLD_REL_I AS REL_I,
		{{ ref('SrcTmpFAClientUndertakingTera') }}.OLD_SRCE_SYST_PATY_INT_GRUP_I AS SRCE_SYST_PATY_INT_GRUP_I,
		{{ ref('SrcTmpFAClientUndertakingTera') }}.OLD_ORIG_SRCE_SYST_PATY_I AS ORIG_SRCE_SYST_PATY_I,
		{{ ref('SrcTmpFAClientUndertakingTera') }}.OLD_ORIG_SRCE_SYST_PATY_TYPE_C AS ORIG_SRCE_SYST_PATY_TYPE_C,
		{{ ref('SrcTmpFAClientUndertakingTera') }}.OLD_PRIM_CLNT_F AS PRIM_CLNT_F,
		{{ ref('SrcTmpFAClientUndertakingTera') }}.OLD_PATY_I AS PATY_I,
		{{ ref('SrcTmpFAClientUndertakingTera') }}.OLD_SRCE_SYST_C AS SRCE_SYST_C,
		OLD_INT_GRUP_I,
		OLD_REL_I,
		OLD_SRCE_SYST_PATY_INT_GRUP_I,
		OLD_ORIG_SRCE_SYST_PATY_I,
		OLD_ORIG_SRCE_SYST_PATY_TYPE_C,
		OLD_PATY_I,
		OLD_EFFT_D,
		OLD_PRIM_CLNT_F,
		OLD_SRCE_SYST_C
	FROM {{ ref('SrcTmpFAClientUndertakingTera') }}
)

SELECT * FROM CpyFAUndertaking