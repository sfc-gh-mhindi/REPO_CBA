{{ config(materialized='view', tags=['DltAPPT_PDCT_CPGNFrmTMP_APPT_PDCT_CPGN']) }}

WITH xf_delta__ln_updates AS (
	SELECT
		APPT_PDCT_I,
		CPGN_TYPE_C,
		SRCE_SYST_C,
		EFFT_D,
		-- *SRC*: \(20)If (InTmpApptPdctCpgnTera.EFFT_D < StringToDate(ETL_PROCESS_DT, "%yyyy%mm%dd")) Then DateFromDaysSince(-1, StringToDate(ETL_PROCESS_DT, "%yyyy%mm%dd")) Else StringToDate(ETL_PROCESS_DT, "%yyyy%mm%dd"),
		IFF({{ ref('SrcTmpApptPdctCpgnTera') }}.EFFT_D < STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd'), DATEFROMDAYSSINCE(-1, STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')), STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')) AS EXPY_D,
		REFR_PK AS PROS_KEY_EXPY_I
	FROM {{ ref('SrcTmpApptPdctCpgnTera') }}
	WHERE {{ ref('SrcTmpApptPdctCpgnTera') }}.IND_FLAG = 'U'
)

SELECT * FROM xf_delta__ln_updates