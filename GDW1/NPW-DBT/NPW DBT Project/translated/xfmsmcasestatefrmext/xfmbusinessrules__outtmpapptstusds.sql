{{ config(materialized='view', tags=['XfmSmCaseStateFrmExt']) }}

WITH XfmBusinessRules__OutTmpApptStusDS AS (
	SELECT
		-- *SRC*: \(20)If Len(Trim(( IF IsNotNull((InXfmBusinessRules.START_DATE)) THEN (InXfmBusinessRules.START_DATE) ELSE ""))) = 0 then 'Y' else 'N',
		IFF(LEN(TRIM(IFF({{ ref('ModNullHandling') }}.START_DATE IS NOT NULL, {{ ref('ModNullHandling') }}.START_DATE, ''))) = 0, 'Y', 'N') AS StartDtIsNull,
		-- *SRC*: \(20)If Trim(InXfmBusinessRules.END_DATE) = '9999' then 'Y' Else 'N',
		IFF(TRIM({{ ref('ModNullHandling') }}.END_DATE) = '9999', 'Y', 'N') AS EndDtIsNull,
		-- *SRC*: \(20)If (StartDtIsNull = 'N') then ( if IsValid('timestamp', StringToTimestamp((trim(InXfmBusinessRules.START_DATE)), '%yyyy%mm%dd%hh%nn%ss')) Then 'N' Else 'Y') Else 'N',
		IFF(StartDtIsNull = 'N', IFF(ISVALID('timestamp', STRINGTOTIMESTAMP(TRIM({{ ref('ModNullHandling') }}.START_DATE), '%yyyy%mm%dd%hh%nn%ss')), 'N', 'Y'), 'N') AS ErrorStartDt,
		-- *SRC*: \(20)If (EndDtIsNull = 'N') then ( if IsValid('timestamp', StringToTimestamp((trim(( IF IsNotNull((InXfmBusinessRules.END_DATE)) THEN (InXfmBusinessRules.END_DATE) ELSE ""))), '%yyyy%mm%dd%hh%nn%ss')) Then 'N' Else 'Y') Else 'N',
		IFF(EndDtIsNull = 'N', IFF(ISVALID('timestamp', STRINGTOTIMESTAMP(TRIM(IFF({{ ref('ModNullHandling') }}.END_DATE IS NOT NULL, {{ ref('ModNullHandling') }}.END_DATE, '')), '%yyyy%mm%dd%hh%nn%ss')), 'N', 'Y'), 'N') AS ErrorEndDt,
		-- *SRC*: \(20)If InXfmBusinessRules.STUS_C = '9999' then 'RPR5108' else '',
		IFF({{ ref('ModNullHandling') }}.STUS_C = '9999', 'RPR5108', '') AS svErrorCode,
		-- *SRC*: trim(InXfmBusinessRules.targ_i),
		TRIM({{ ref('ModNullHandling') }}.targ_i) AS APPT_I,
		STUS_C,
		-- *SRC*: \(20)If ErrorStartDt = 'N' Then StringToTimestamp((trim(InXfmBusinessRules.START_DATE)), "%yyyy%mm%dd%hh%nn%ss") Else StringToTimestamp(DEFAULT_DT : " 000000", "%yyyy%mm%dd %hh%nn%ss"),
		IFF(ErrorStartDt = 'N', STRINGTOTIMESTAMP(TRIM({{ ref('ModNullHandling') }}.START_DATE), '%yyyy%mm%dd%hh%nn%ss'), STRINGTOTIMESTAMP(CONCAT(DEFAULT_DT, ' 000000'), '%yyyy%mm%dd %hh%nn%ss')) AS STRT_S,
		-- *SRC*: \(20)If ErrorStartDt = 'N' Then StringToDate((trim(InXfmBusinessRules.START_DATE))[1, 8], "%yyyy%mm%dd") Else StringToDate(DEFAULT_DT, "%yyyy%mm%dd"),
		IFF(ErrorStartDt = 'N', STRINGTODATE(TRIM({{ ref('ModNullHandling') }}.START_DATE), '%yyyy%mm%dd'), STRINGTODATE(DEFAULT_DT, '%yyyy%mm%dd')) AS STRT_D,
		-- *SRC*: \(20)If ErrorStartDt = 'N' Then (trim(InXfmBusinessRules.START_DATE))[9, 6] Else '00:00:00',
		IFF(ErrorStartDt = 'N', TRIM({{ ref('ModNullHandling') }}.START_DATE), '00:00:00') AS STRT_T,
		-- *SRC*: \(20)If EndDtIsNull = 'Y' then SetNull() Else  If ErrorEndDt = 'N' Then StringToDate((trim(InXfmBusinessRules.END_DATE))[1, 8], "%yyyy%mm%dd") Else StringToDate(DEFAULT_DT, "%yyyy%mm%dd"),
		IFF(EndDtIsNull = 'Y', SETNULL(), IFF(ErrorEndDt = 'N', STRINGTODATE(TRIM({{ ref('ModNullHandling') }}.END_DATE), '%yyyy%mm%dd'), STRINGTODATE(DEFAULT_DT, '%yyyy%mm%dd'))) AS END_D,
		-- *SRC*: \(20)If EndDtIsNull = 'Y' then SetNull() Else  If ErrorEndDt = 'N' Then (trim(InXfmBusinessRules.END_DATE))[9, 6] Else SetNull(),
		IFF(EndDtIsNull = 'Y', SETNULL(), IFF(ErrorEndDt = 'N', TRIM({{ ref('ModNullHandling') }}.END_DATE), SETNULL())) AS END_T,
		-- *SRC*: \(20)If EndDtIsNull = 'Y' then SetNull() Else  If ErrorEndDt = 'N' Then StringToTimestamp((trim(InXfmBusinessRules.END_DATE)), "%yyyy%mm%dd%hh%nn%ss") Else StringToTimestamp(DEFAULT_DT : " 000000", "%yyyy%mm%dd %hh%nn%ss"),
		IFF(EndDtIsNull = 'Y', SETNULL(), IFF(ErrorEndDt = 'N', STRINGTOTIMESTAMP(TRIM({{ ref('ModNullHandling') }}.END_DATE), '%yyyy%mm%dd%hh%nn%ss'), STRINGTOTIMESTAMP(CONCAT(DEFAULT_DT, ' 000000'), '%yyyy%mm%dd %hh%nn%ss'))) AS END_S,
		{{ ref('ModNullHandling') }}.CREATED_BY_STAFF_NUMBER AS EMPL_I,
		-- *SRC*: StringToDate(ETL_PROCESS_DT, "%yyyy%mm%dd"),
		STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd') AS EFFT_D,
		-- *SRC*: StringToDate('99991231', "%yyyy%mm%dd"),
		STRINGTODATE('99991231', '%yyyy%mm%dd') AS EXPY_D,
		0 AS PROS_KEY_EFFT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS PROS_KEY_EXPY_I,
		-- *SRC*: SetNull(),
		SETNULL() AS EROR_SEQN_I,
		RUN_STREAM AS RUN_STRM
	FROM {{ ref('ModNullHandling') }}
	WHERE TRIM({{ ref('ModNullHandling') }}.targ_tabl) = 'APPT'
)

SELECT * FROM XfmBusinessRules__OutTmpApptStusDS