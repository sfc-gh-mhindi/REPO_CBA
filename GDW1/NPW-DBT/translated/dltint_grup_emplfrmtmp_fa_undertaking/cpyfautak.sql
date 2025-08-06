{{ config(materialized='view', tags=['DltINT_GRUP_EMPLFrmTMP_FA_UNDERTAKING']) }}

WITH CpyFAUtak AS (
	SELECT
		INT_GRUP_I,
		{{ ref('SrcTmpFAUtakTera') }}.OLD_INT_GRUP_I AS INT_GRUP_I,
		EMPL_I,
		EMPL_ROLE_C
	FROM {{ ref('SrcTmpFAUtakTera') }}
)

SELECT * FROM CpyFAUtak