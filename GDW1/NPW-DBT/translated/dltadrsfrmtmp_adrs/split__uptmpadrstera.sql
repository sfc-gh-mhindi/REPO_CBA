{{ config(materialized='view', tags=['DltADRSFrmTMP_ADRS']) }}

WITH Split__UpTmpADRSTera AS (
	SELECT
		ADRS_I,
		ADRS_TYPE_C,
		SRCE_SYST_C,
		ADRS_QLFY_C,
		SRCE_SYST_ADRS_I,
		SRCE_SYST_ADRS_SEQN_N
	FROM {{ ref('SrcTmpADRSTera') }}
	WHERE 1 = 2
)

SELECT * FROM Split__UpTmpADRSTera