{{ config(materialized='view', tags=['DltINT_GRUPFrmTMP_FA_UNDERTAKING']) }}

WITH JoinAll AS (
	SELECT
		{{ ref('ChangeCapture') }}.INT_GRUP_I,
		{{ ref('ChangeCapture') }}.ORIG_SRCE_SYST_INT_GRUP_I,
		{{ ref('ChangeCapture') }}.INT_GRUP_M,
		{{ ref('ChangeCapture') }}.change_code,
		{{ ref('CpyFAUndertaking') }}.INT_GRUP_TYPE_C,
		{{ ref('CpyFAUndertaking') }}.SRCE_SYST_INT_GRUP_I,
		{{ ref('CpyFAUndertaking') }}.SRCE_SYST_C,
		{{ ref('CpyFAUndertaking') }}.CRAT_D,
		{{ ref('CpyFAUndertaking') }}.OLD_INT_GRUP_I,
		{{ ref('CpyFAUndertaking') }}.OLD_INT_GRUP_M,
		{{ ref('CpyFAUndertaking') }}.OLD_ORIG_SRCE_SYST_INT_GRUP_I,
		{{ ref('CpyFAUndertaking') }}.OLD_EFFT_D
	FROM {{ ref('ChangeCapture') }}
	INNER JOIN {{ ref('CpyFAUndertaking') }} ON {{ ref('ChangeCapture') }}.INT_GRUP_I = {{ ref('CpyFAUndertaking') }}.INT_GRUP_I
)

SELECT * FROM JoinAll