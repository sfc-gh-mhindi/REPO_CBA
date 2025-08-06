{{ config(materialized='view', tags=['XfmFaUndertakingFrmExt']) }}

WITH XfmBusinessRules__OutSmCaseDS AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((InXfmBusinessRules.SM_CASE_ID)) THEN (InXfmBusinessRules.SM_CASE_ID) ELSE ""))) = 0) then 'Y' else 'N',
		IFF(LEN(TRIM(IFF({{ ref('CpyRename') }}.SM_CASE_ID IS NOT NULL, {{ ref('CpyRename') }}.SM_CASE_ID, ''))) = 0, 'Y', 'N') AS SmCaseIdIsNull,
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((InXfmBusinessRules.CREATED_BY_STAFF_NUMBER)) THEN (InXfmBusinessRules.CREATED_BY_STAFF_NUMBER) ELSE ""))) = 0) then 'N' else 'Y',
		IFF(LEN(TRIM(IFF({{ ref('CpyRename') }}.CREATED_BY_STAFF_NUMBER IS NOT NULL, {{ ref('CpyRename') }}.CREATED_BY_STAFF_NUMBER, ''))) = 0, 'N', 'Y') AS IntGrupEmplF,
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((InXfmBusinessRules.CREATED_DATE)) THEN (InXfmBusinessRules.CREATED_DATE) ELSE ""))) = 0) then 'Y' else 'N',
		IFF(LEN(TRIM(IFF({{ ref('CpyRename') }}.CREATED_DATE IS NOT NULL, {{ ref('CpyRename') }}.CREATED_DATE, ''))) = 0, 'Y', 'N') AS CratDateIsNull,
		-- *SRC*: \(20)If CratDateIsNull = 'Y' then 'N' else ( if IsValid('date', StringToDate(trim(InXfmBusinessRules.CREATED_DATE), '%yyyy%mm%dd')) Then 'N' Else 'Y'),
		IFF(CratDateIsNull = 'Y', 'N', IFF(ISVALID('date', STRINGTODATE(TRIM({{ ref('CpyRename') }}.CREATED_DATE), '%yyyy%mm%dd')), 'N', 'Y')) AS ErrorCratDate,
		SM_CASE_ID,
		-- *SRC*: 'CSEC1' : InXfmBusinessRules.FA_UNDERTAKING_ID,
		CONCAT('CSEC1', {{ ref('CpyRename') }}.FA_UNDERTAKING_ID) AS TARG_I,
		'INT_GRUP' AS TARG_SUBJ
	FROM {{ ref('CpyRename') }}
	WHERE SmCaseIdIsNull = 'N'
)

SELECT * FROM XfmBusinessRules__OutSmCaseDS