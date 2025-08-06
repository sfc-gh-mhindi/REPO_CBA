{{ config(materialized='view', tags=['DltADRSFrmTMP_ADRS']) }}

WITH Split__InTmpADRSTera AS (
	SELECT
		ADRS_I,
		ADRS_TYPE_C,
		SRCE_SYST_C,
		ADRS_QLFY_C,
		SRCE_SYST_ADRS_I,
		SRCE_SYST_ADRS_SEQN_N,
		-- *SRC*: StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd'),
		STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd') AS EFFT_D,
		-- *SRC*: AsInteger(REFR_PK),
		ASINTEGER(REFR_PK) AS PROS_KEY_EFFT_I,
		0 AS ROW_SECU_ACCS_C
	FROM {{ ref('SrcTmpADRSTera') }}
	WHERE 
)

SELECT * FROM Split__InTmpADRSTera