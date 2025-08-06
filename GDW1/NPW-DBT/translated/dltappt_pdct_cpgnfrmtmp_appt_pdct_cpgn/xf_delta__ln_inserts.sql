{{ config(materialized='view', tags=['DltAPPT_PDCT_CPGNFrmTMP_APPT_PDCT_CPGN']) }}

WITH xf_delta__ln_inserts AS (
	SELECT
		APPT_PDCT_I,
		CPGN_TYPE_C,
		CPGN_I,
		REL_C,
		SRCE_SYST_C,
		-- *SRC*: StringToDate(ETL_PROCESS_DT, "%yyyy%mm%dd"),
		STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd') AS EFFT_D,
		'9999-12-31' AS EXPY_D,
		REFR_PK AS PROS_KEY_EFFT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS PROS_KEY_EXPY_I,
		'0' AS ROW_SECU_ACCS_C
	FROM {{ ref('SrcTmpApptPdctCpgnTera') }}
	WHERE {{ ref('SrcTmpApptPdctCpgnTera') }}.IND_FLAG = 'I' OR {{ ref('SrcTmpApptPdctCpgnTera') }}.IND_FLAG = 'U'
)

SELECT * FROM xf_delta__ln_inserts