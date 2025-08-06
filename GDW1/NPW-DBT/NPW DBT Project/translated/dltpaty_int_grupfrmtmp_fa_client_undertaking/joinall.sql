{{ config(materialized='view', tags=['DltPATY_INT_GRUPFrmTMP_FA_CLIENT_UNDERTAKING']) }}

WITH JoinAll AS (
	SELECT
		{{ ref('ChangeCapture') }}.INT_GRUP_I,
		{{ ref('ChangeCapture') }}.ORIG_SRCE_SYST_PATY_I,
		{{ ref('ChangeCapture') }}.PATY_I,
		{{ ref('ChangeCapture') }}.change_code,
		{{ ref('ChangeCapture') }}.REL_I,
		{{ ref('ChangeCapture') }}.SRCE_SYST_PATY_INT_GRUP_I,
		{{ ref('ChangeCapture') }}.ORIG_SRCE_SYST_PATY_TYPE_C,
		{{ ref('ChangeCapture') }}.PRIM_CLNT_F,
		{{ ref('ChangeCapture') }}.SRCE_SYST_C,
		{{ ref('CpyFAUndertaking') }}.OLD_INT_GRUP_I,
		{{ ref('CpyFAUndertaking') }}.OLD_REL_I,
		{{ ref('CpyFAUndertaking') }}.OLD_SRCE_SYST_PATY_INT_GRUP_I,
		{{ ref('CpyFAUndertaking') }}.OLD_ORIG_SRCE_SYST_PATY_I,
		{{ ref('CpyFAUndertaking') }}.OLD_ORIG_SRCE_SYST_PATY_TYPE_C,
		{{ ref('CpyFAUndertaking') }}.OLD_PATY_I,
		{{ ref('CpyFAUndertaking') }}.OLD_EFFT_D,
		{{ ref('CpyFAUndertaking') }}.OLD_PRIM_CLNT_F,
		{{ ref('CpyFAUndertaking') }}.OLD_SRCE_SYST_C
	FROM {{ ref('ChangeCapture') }}
	INNER JOIN {{ ref('CpyFAUndertaking') }} ON {{ ref('ChangeCapture') }}.INT_GRUP_I = {{ ref('CpyFAUndertaking') }}.INT_GRUP_I
	AND {{ ref('ChangeCapture') }}.SRCE_SYST_PATY_INT_GRUP_I = {{ ref('CpyFAUndertaking') }}.SRCE_SYST_PATY_INT_GRUP_I
)

SELECT * FROM JoinAll