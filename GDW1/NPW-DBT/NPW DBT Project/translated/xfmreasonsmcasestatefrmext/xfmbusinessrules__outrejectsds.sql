{{ config(materialized='view', tags=['XfmReasonSmCaseStateFrmExt']) }}

WITH XfmBusinessRules__OutRejectsDS AS (
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
		SM_CASE_STATE_ID,
		{{ ref('ModNullHandling') }}.SM_CASE_ID AS SCS_SM_CASE_ID,
		{{ ref('ModNullHandling') }}.SM_STATE_CAT_ID AS SCS_SM_STATE_CAT_ID,
		SCS_START_DATE,
		SCS_END_DATE,
		SCS_CREATED_BY_STAFF_NUMBER,
		SCS_STATE_CAUSED_BY_ACTION_ID,
		SCSR_SM_CASE_STATE_REASON_ID,
		{{ ref('ModNullHandling') }}.SM_REAS_CAT_ID AS SCSR_SM_REASON_CAT_ID,
		SM_CASE_STATE_REAS_FOUND_FLAG,
		SM_CASE_STATE_FOUND_FLAG,
		ETL_PROCESS_DT AS ETL_D,
		ORIG_ETL_D,
		svErrorCode AS EROR_C
	FROM {{ ref('ModNullHandling') }}
	WHERE svErrorCode <> ''
)

SELECT * FROM XfmBusinessRules__OutRejectsDS