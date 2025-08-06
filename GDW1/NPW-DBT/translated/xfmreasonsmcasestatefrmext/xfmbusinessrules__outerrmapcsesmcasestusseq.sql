{{ config(materialized='view', tags=['XfmReasonSmCaseStateFrmExt']) }}

WITH XfmBusinessRules__OutErrMapCseSmCaseStusSeq AS (
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
		{{ ref('ModNullHandling') }}.SM_CASE_STATE_ID AS SRCE_KEY_I,
		'SM_STATE_CAT_ID' AS CONV_M,
		'LOOKUP' AS CONV_MAP_RULE_M,
		'MAP_CSE_SM_CASE_STUS' AS TRSF_TABL_M,
		{{ ref('ModNullHandling') }}.SM_STATE_CAT_ID AS VALU_CHNG_BFOR_X,
		{{ ref('ModNullHandling') }}.STUS_C AS VALU_CHNG_AFTR_X,
		-- *SRC*: 'Job Name = ' : DSJobName,
		CONCAT('Job Name = ', DSJobName) AS TRSF_X,
		'STUS_C' AS TRSF_COLM_M
	FROM {{ ref('ModNullHandling') }}
	WHERE {{ ref('ModNullHandling') }}.STUS_C = '9999' AND TRIM({{ ref('ModNullHandling') }}.TARG_SUBJ) = 'INT_GRUP' OR TRIM({{ ref('ModNullHandling') }}.TARG_SUBJ) = 'APPT'
)

SELECT * FROM XfmBusinessRules__OutErrMapCseSmCaseStusSeq