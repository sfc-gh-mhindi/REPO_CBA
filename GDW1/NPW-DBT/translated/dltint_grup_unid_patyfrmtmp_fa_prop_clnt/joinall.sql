{{ config(materialized='view', tags=['DltINT_GRUP_UNID_PATYFrmTMP_FA_PROP_CLNT']) }}

WITH JoinAll AS (
	SELECT
		{{ ref('ChangeCapture') }}.INT_GRUP_I,
		{{ ref('ChangeCapture') }}.SRCE_SYST_PATY_I,
		{{ ref('ChangeCapture') }}.delta_gdw_change_code,
		{{ ref('CpyFAPropClnt') }}.ORIG_SRCE_SYST_PATY_I,
		{{ ref('CpyFAPropClnt') }}.UNID_PATY_M,
		{{ ref('CpyFAPropClnt') }}.PATY_TYPE_C,
		{{ ref('CpyFAPropClnt') }}.SRCE_SYST_C,
		{{ ref('CpyFAPropClnt') }}.CHNG_CODE,
		{{ ref('CpyFAPropClnt') }}.OLD_INT_GRUP_I,
		{{ ref('CpyFAPropClnt') }}.OLD_SRCE_SYST_PATY_I,
		{{ ref('CpyFAPropClnt') }}.OLD_ORIG_SRCE_SYST_PATY_I,
		{{ ref('CpyFAPropClnt') }}.OLD_UNID_PATY_M,
		{{ ref('CpyFAPropClnt') }}.OLD_PATY_TYPE_C,
		{{ ref('CpyFAPropClnt') }}.OLD_EFFT_D
	FROM {{ ref('ChangeCapture') }}
	INNER JOIN {{ ref('CpyFAPropClnt') }} ON {{ ref('ChangeCapture') }}.INT_GRUP_I = {{ ref('CpyFAPropClnt') }}.INT_GRUP_I
	AND {{ ref('ChangeCapture') }}.SRCE_SYST_PATY_I = {{ ref('CpyFAPropClnt') }}.SRCE_SYST_PATY_I
)

SELECT * FROM JoinAll