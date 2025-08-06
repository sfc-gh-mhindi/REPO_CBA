{{ config(materialized='view', tags=['DltCSE_CMLN_APPT_XPOS_ASES_DETLFrmTMP']) }}

WITH xf_delta__ln_inserts AS (
	SELECT
		APPT_I,
		XPOS_A,
		XPOS_AMT_D,
		OVRD_COVTS_ASES_F,
		CSE_CMLN_OVRD_REAS_CATG_C,
		SHRT_DFLT_OVRD_F,
		-- *SRC*: StringToDate(ETL_PROCESS_DT, "%yyyy%mm%dd"),
		STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd') AS EFFT_D,
		'9999-12-31' AS EXPY_D,
		REFR_PK AS PROS_KEY_EFFT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS PROS_KEY_EXPY_I
	FROM {{ ref('SrcTmpCseCmlnApptXposAsesDetlTera') }}
	WHERE {{ ref('SrcTmpCseCmlnApptXposAsesDetlTera') }}.IND_FLAG = 'I' OR {{ ref('SrcTmpCseCmlnApptXposAsesDetlTera') }}.IND_FLAG = 'U'
)

SELECT * FROM xf_delta__ln_inserts