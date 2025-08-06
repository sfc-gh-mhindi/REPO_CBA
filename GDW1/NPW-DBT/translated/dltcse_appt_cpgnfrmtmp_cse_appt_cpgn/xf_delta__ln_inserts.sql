{{ config(materialized='view', tags=['DltCSE_APPT_CPGNFrmTMP_CSE_APPT_CPGN']) }}

WITH xf_delta__ln_inserts AS (
	SELECT
		APPT_I,
		CSE_CPGN_CODE_X,
		-- *SRC*: StringToDate(ETL_PROCESS_DT, "%yyyy%mm%dd"),
		STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd') AS EFFT_D,
		'9999-12-31' AS EXPY_D,
		REFR_PK AS PROS_KEY_EFFT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS PROS_KEY_EXPY_I,
		0 AS ROW_SECU_ACCS_C
	FROM {{ ref('SrcTmpCseApptCpgnTera') }}
	WHERE {{ ref('SrcTmpCseApptCpgnTera') }}.INSERT_UPDATE_FLAG = 'I' OR {{ ref('SrcTmpCseApptCpgnTera') }}.INSERT_UPDATE_FLAG = 'U'
)

SELECT * FROM xf_delta__ln_inserts