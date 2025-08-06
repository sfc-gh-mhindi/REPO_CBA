{{ config(materialized='view', tags=['DltCSE_CMLN_APPT_XPOS_ASES_DETLFrmTMP']) }}

WITH xf_delta__ln_updates AS (
	SELECT
		APPT_I,
		EFFT_D,
		-- *SRC*: \(20)If (InTmpCseCmlnApptXposAsesDetlTera.EFFT_D < StringToDate(ETL_PROCESS_DT, "%yyyy%mm%dd")) Then DateFromDaysSince(-1, StringToDate(ETL_PROCESS_DT, "%yyyy%mm%dd")) Else StringToDate(ETL_PROCESS_DT, "%yyyy%mm%dd"),
		IFF({{ ref('SrcTmpCseCmlnApptXposAsesDetlTera') }}.EFFT_D < STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd'), DATEFROMDAYSSINCE(-1, STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')), STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')) AS EXPY_D,
		REFR_PK AS PROS_KEY_EXPY_I
	FROM {{ ref('SrcTmpCseCmlnApptXposAsesDetlTera') }}
	WHERE {{ ref('SrcTmpCseCmlnApptXposAsesDetlTera') }}.IND_FLAG = 'U'
)

SELECT * FROM xf_delta__ln_updates