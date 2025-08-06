{{ config(materialized='view', tags=['XfmFaUndertakingFrmExt']) }}

WITH XfmBusinessRules__OutUTakDS AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((InXfmBusinessRules.SM_CASE_ID)) THEN (InXfmBusinessRules.SM_CASE_ID) ELSE ""))) = 0) then 'Y' else 'N',
		IFF(LEN(TRIM(IFF({{ ref('CpyRename') }}.SM_CASE_ID IS NOT NULL, {{ ref('CpyRename') }}.SM_CASE_ID, ''))) = 0, 'Y', 'N') AS SmCaseIdIsNull,
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((InXfmBusinessRules.CREATED_BY_STAFF_NUMBER)) THEN (InXfmBusinessRules.CREATED_BY_STAFF_NUMBER) ELSE ""))) = 0) then 'N' else 'Y',
		IFF(LEN(TRIM(IFF({{ ref('CpyRename') }}.CREATED_BY_STAFF_NUMBER IS NOT NULL, {{ ref('CpyRename') }}.CREATED_BY_STAFF_NUMBER, ''))) = 0, 'N', 'Y') AS IntGrupEmplF,
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((InXfmBusinessRules.CREATED_DATE)) THEN (InXfmBusinessRules.CREATED_DATE) ELSE ""))) = 0) then 'Y' else 'N',
		IFF(LEN(TRIM(IFF({{ ref('CpyRename') }}.CREATED_DATE IS NOT NULL, {{ ref('CpyRename') }}.CREATED_DATE, ''))) = 0, 'Y', 'N') AS CratDateIsNull,
		-- *SRC*: \(20)If CratDateIsNull = 'Y' then 'N' else ( if IsValid('date', StringToDate(trim(InXfmBusinessRules.CREATED_DATE), '%yyyy%mm%dd')) Then 'N' Else 'Y'),
		IFF(CratDateIsNull = 'Y', 'N', IFF(ISVALID('date', STRINGTODATE(TRIM({{ ref('CpyRename') }}.CREATED_DATE), '%yyyy%mm%dd')), 'N', 'Y')) AS ErrorCratDate,
		{{ ref('CpyRename') }}.FA_UNDERTAKING_ID AS FA_UTAK_ID,
		{{ ref('CpyRename') }}.PLANNING_GROUP_NAME AS PLAN_GRP_NAME,
		{{ ref('CpyRename') }}.COIN_ADVICE_GROUP_ID AS COIN_ADVC_GRP_ID,
		{{ ref('CpyRename') }}.ADVICE_GROUP_CORRELATION_ID AS ADVC_GRP_CORL_ID,
		{{ ref('CpyRename') }}.CREATED_DATE AS CRAT_DATE,
		{{ ref('CpyRename') }}.CREATED_BY_STAFF_NUMBER AS CRAT_BY_STAF_NUM,
		SM_CASE_ID,
		ORIG_ETL_D,
		-- *SRC*: 'CSEC1' : InXfmBusinessRules.FA_UNDERTAKING_ID,
		CONCAT('CSEC1', {{ ref('CpyRename') }}.FA_UNDERTAKING_ID) AS INT_GRUP_I,
		'FNPG' AS INT_GRUP_TYPE_C,
		{{ ref('CpyRename') }}.PLANNING_GROUP_NAME AS INT_GRUP_M,
		{{ ref('CpyRename') }}.FA_UNDERTAKING_ID AS SRCE_SYST_INT_GRUP_I,
		'CSE' AS SRCE_SYST_C,
		{{ ref('CpyRename') }}.COIN_ADVICE_GROUP_ID AS ORIG_SRCE_SYST_INT_GRUP_I,
		-- *SRC*: \(20)If ErrorCratDate = 'N' Then StringToDate(trim(InXfmBusinessRules.CREATED_DATE), "%yyyy%mm%dd") Else StringToDate(DEFAULT_DT, "%yyyy%mm%dd"),
		IFF(ErrorCratDate = 'N', STRINGTODATE(TRIM({{ ref('CpyRename') }}.CREATED_DATE), '%yyyy%mm%dd'), STRINGTODATE(DEFAULT_DT, '%yyyy%mm%dd')) AS CRAT_D,
		-- *SRC*: ( IF IsNotNull((InXfmBusinessRules.CREATED_BY_STAFF_NUMBER)) THEN (InXfmBusinessRules.CREATED_BY_STAFF_NUMBER) ELSE ('00000000')),
		IFF({{ ref('CpyRename') }}.CREATED_BY_STAFF_NUMBER IS NOT NULL, {{ ref('CpyRename') }}.CREATED_BY_STAFF_NUMBER, '00000000') AS EMPL_I,
		'FNP' AS EMPL_ROLE_C,
		IntGrupEmplF AS INT_GRUP_EMPL_F
	FROM {{ ref('CpyRename') }}
	WHERE 
)

SELECT * FROM XfmBusinessRules__OutUTakDS