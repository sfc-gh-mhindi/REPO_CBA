{{ config(materialized='view', tags=['DltINT_GRUPFrmTMP_FA_UNDERTAKING']) }}

WITH CpyFAUndertaking AS (
	SELECT
		INT_GRUP_I,
		ORIG_SRCE_SYST_INT_GRUP_I,
		INT_GRUP_M,
		{{ ref('SrcTmpFAUtakTera') }}.OLD_INT_GRUP_I AS INT_GRUP_I,
		{{ ref('SrcTmpFAUtakTera') }}.OLD_ORIG_SRCE_SYST_INT_GRUP_I AS ORIG_SRCE_SYST_INT_GRUP_I,
		{{ ref('SrcTmpFAUtakTera') }}.OLD_INT_GRUP_M AS INT_GRUP_M,
		INT_GRUP_TYPE_C,
		SRCE_SYST_INT_GRUP_I,
		SRCE_SYST_C,
		CRAT_D,
		OLD_INT_GRUP_I,
		OLD_INT_GRUP_M,
		OLD_ORIG_SRCE_SYST_INT_GRUP_I,
		OLD_EFFT_D
	FROM {{ ref('SrcTmpFAUtakTera') }}
)

SELECT * FROM CpyFAUndertaking