{{ config(materialized='view', tags=['XfmSmCaseStateFrmExt']) }}

WITH XfmBusinessRules__OutErrorEndDate AS (
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
		{{ ref('ModNullHandling') }}.SM_CASE_STATE_ID AS SRCE_KEY_I,
		'ConversionTimestamp' AS CONV_M,
		'SRCTRMCHECK' AS CONV_MAP_RULE_M,
		'N/A' AS TRSF_TABL_M,
		{{ ref('ModNullHandling') }}.END_DATE AS VALU_CHNG_BFOR_X,
		'1111-11-11' AS VALU_CHNG_AFTR_X,
		-- *SRC*: 'Job Name = ' : DSJobName,
		CONCAT('Job Name = ', DSJobName) AS TRSF_X,
		'END_D' AS TRSF_COLM_M
	FROM {{ ref('ModNullHandling') }}
	WHERE ErrorEndDt = 'Y'
)

SELECT * FROM XfmBusinessRules__OutErrorEndDate