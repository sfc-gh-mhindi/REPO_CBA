{{ config(materialized='view', tags=['XfmReasonSmCaseStateFrmExt']) }}

WITH XfmBusinessRules__OutTmpApptStusReasDS AS (
	SELECT
		-- *SRC*: \(20)If Len(Trim(( IF IsNotNull((InXfmBusinessRules.SCS_START_DATE)) THEN (InXfmBusinessRules.SCS_START_DATE) ELSE ""))) = 0 then 'Y' else 'N',
		IFF(LEN(TRIM(IFF({{ ref('ModNullHandling') }}.SCS_START_DATE IS NOT NULL, {{ ref('ModNullHandling') }}.SCS_START_DATE, ''))) = 0, 'Y', 'N') AS StartDtIsNull,
		-- *SRC*: \(20)If Trim(InXfmBusinessRules.SCS_END_DATE) = '9999' then 'Y' else 'N',
		IFF(TRIM({{ ref('ModNullHandling') }}.SCS_END_DATE) = '9999', 'Y', 'N') AS EndDtIsNull,
		-- *SRC*: \(20)If (StartDtIsNull = 'N') then ( if IsValid('timestamp', StringToTimestamp((trim(InXfmBusinessRules.SCS_START_DATE)), '%yyyy%mm%dd%hh%nn%ss')) Then 'N' Else 'Y') Else 'N',
		IFF(StartDtIsNull = 'N', IFF(ISVALID('timestamp', STRINGTOTIMESTAMP(TRIM({{ ref('ModNullHandling') }}.SCS_START_DATE), '%yyyy%mm%dd%hh%nn%ss')), 'N', 'Y'), 'N') AS ErrorStartDt,
		-- *SRC*: \(20)If (EndDtIsNull = 'N') then ( if IsValid('timestamp', StringToTimestamp((trim(InXfmBusinessRules.SCS_END_DATE)), '%yyyy%mm%dd%hh%nn%ss')) Then 'N' Else 'Y') Else 'N',
		IFF(EndDtIsNull = 'N', IFF(ISVALID('timestamp', STRINGTOTIMESTAMP(TRIM({{ ref('ModNullHandling') }}.SCS_END_DATE), '%yyyy%mm%dd%hh%nn%ss')), 'N', 'Y'), 'N') AS ErrorEndDt,
		-- *SRC*: \(20)If InXfmBusinessRules.STUS_C = '9999' then 'RPR5108' else  if InXfmBusinessRules.STUS_REAS_TYPE_C = '9999' then 'RPR5109' else '',
		IFF({{ ref('ModNullHandling') }}.STUS_C = '9999', 'RPR5108', IFF({{ ref('ModNullHandling') }}.STUS_REAS_TYPE_C = '9999', 'RPR5109', '')) AS svErrorCode,
		-- *SRC*: trim(InXfmBusinessRules.TARG_I),
		TRIM({{ ref('ModNullHandling') }}.TARG_I) AS APPT_I,
		STUS_C,
		STUS_REAS_TYPE_C,
		-- *SRC*: \(20)If ErrorStartDt = 'N' Then StringToTimestamp((trim(InXfmBusinessRules.SCS_START_DATE)), "%yyyy%mm%dd%hh%nn%ss") Else StringToTimestamp(DEFAULT_DT : " 000000", "%yyyy%mm%dd %hh%nn%ss"),
		IFF(ErrorStartDt = 'N', STRINGTOTIMESTAMP(TRIM({{ ref('ModNullHandling') }}.SCS_START_DATE), '%yyyy%mm%dd%hh%nn%ss'), STRINGTOTIMESTAMP(CONCAT(DEFAULT_DT, ' 000000'), '%yyyy%mm%dd %hh%nn%ss')) AS STRT_S,
		-- *SRC*: \(20)If EndDtIsNull = 'Y' then SetNull() else  If ErrorEndDt = 'N' Then StringToTimestamp((trim(( IF IsNotNull((InXfmBusinessRules.SCS_END_DATE)) THEN (InXfmBusinessRules.SCS_END_DATE) ELSE ""))), "%yyyy%mm%dd%hh%nn%ss") else StringToTimestamp(DEFAULT_DT : " 000000", "%yyyy%mm%dd %hh%nn%ss"),
		IFF(EndDtIsNull = 'Y', SETNULL(), IFF(ErrorEndDt = 'N', STRINGTOTIMESTAMP(TRIM(IFF({{ ref('ModNullHandling') }}.SCS_END_DATE IS NOT NULL, {{ ref('ModNullHandling') }}.SCS_END_DATE, '')), '%yyyy%mm%dd%hh%nn%ss'), STRINGTOTIMESTAMP(CONCAT(DEFAULT_DT, ' 000000'), '%yyyy%mm%dd %hh%nn%ss'))) AS END_S,
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
	WHERE TRIM({{ ref('ModNullHandling') }}.TARG_SUBJ) = 'APPT' AND {{ ref('ModNullHandling') }}.SM_CASE_STATE_REAS_FOUND_FLAG = 'Y'
)

SELECT * FROM XfmBusinessRules__OutTmpApptStusReasDS