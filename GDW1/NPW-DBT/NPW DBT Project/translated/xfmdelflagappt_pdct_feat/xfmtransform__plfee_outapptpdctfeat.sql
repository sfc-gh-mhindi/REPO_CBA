{{ config(materialized='view', tags=['XfmDelFlagAPPT_PDCT_FEAT']) }}

WITH XfmTransform__PLFEE_OutApptPdctFeat AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((InModNullHandling.DUMMY_PDCT_F)) THEN (InModNullHandling.DUMMY_PDCT_F) ELSE ""))) = 0) Then 'Y' Else 'N',
		IFF(LEN(TRIM(IFF({{ ref('LkpReferences') }}.DUMMY_PDCT_F IS NOT NULL, {{ ref('LkpReferences') }}.DUMMY_PDCT_F, ''))) = 0, 'Y', 'N') AS svIsExistAppProdId,
		-- *SRC*: \(20)If InModNullHandling.DELETED_TABLE_NAME = 'CC_APP_PROD' Then 'Y' Else 'N',
		IFF({{ ref('LkpReferences') }}.DELETED_TABLE_NAME = 'CC_APP_PROD', 'Y', 'N') AS svLoadCC,
		-- *SRC*: \(20)If InModNullHandling.DELETED_TABLE_NAME = 'CCL_APP_PROD' Then 'Y' Else 'N',
		IFF({{ ref('LkpReferences') }}.DELETED_TABLE_NAME = 'CCL_APP_PROD', 'Y', 'N') AS svLoadCCL,
		-- *SRC*: \(20)If (InModNullHandling.DELETED_TABLE_NAME = 'HL_APP_PROD' Or InModNullHandling.DELETED_TABLE_NAME = 'PL_APP_PROD') Then 'Y' Else 'N',
		IFF({{ ref('LkpReferences') }}.DELETED_TABLE_NAME = 'HL_APP_PROD' OR {{ ref('LkpReferences') }}.DELETED_TABLE_NAME = 'PL_APP_PROD', 'Y', 'N') AS svLoadHLPL,
		-- *SRC*: \(20)If InModNullHandling.DELETED_TABLE_NAME = 'HL_FEATURE_ATTR' Then 'Y' Else 'N',
		IFF({{ ref('LkpReferences') }}.DELETED_TABLE_NAME = 'HL_FEATURE_ATTR', 'Y', 'N') AS svLoadATTR,
		-- *SRC*: \(20)If InModNullHandling.DELETED_TABLE_NAME = 'PL_FEE' Then 'Y' Else 'N',
		IFF({{ ref('LkpReferences') }}.DELETED_TABLE_NAME = 'PL_FEE', 'Y', 'N') AS svLoadPLFEE,
		-- *SRC*: \(20)If InModNullHandling.DELETED_TABLE_NAME = 'CCL_APP_FEE' Then 'Y' Else 'N',
		IFF({{ ref('LkpReferences') }}.DELETED_TABLE_NAME = 'CCL_APP_FEE', 'Y', 'N') AS svLoadFEE,
		-- *SRC*: \(20)If (InModNullHandling.DELETED_TABLE_NAME = 'HL_FEE' OR InModNullHandling.DELETED_TABLE_NAME = 'HL_INT_RATE' OR InModNullHandling.DELETED_TABLE_NAME = 'PL_INT_RATE') Then 'Y' Else 'N',
		IFF({{ ref('LkpReferences') }}.DELETED_TABLE_NAME = 'HL_FEE' OR {{ ref('LkpReferences') }}.DELETED_TABLE_NAME = 'HL_INT_RATE' OR {{ ref('LkpReferences') }}.DELETED_TABLE_NAME = 'PL_INT_RATE', 'Y', 'N') AS svJoinPart1,
		-- *SRC*: \(20)If (InModNullHandling.DELETED_TABLE_NAME = 'HL_FEE_DISCOUNT' OR InModNullHandling.DELETED_TABLE_NAME = 'HL_PROD_INT_MARGIN' OR InModNullHandling.DELETED_TABLE_NAME = 'PL_MARGIN') Then 'Y' Else 'N',
		IFF({{ ref('LkpReferences') }}.DELETED_TABLE_NAME = 'HL_FEE_DISCOUNT' OR {{ ref('LkpReferences') }}.DELETED_TABLE_NAME = 'HL_PROD_INT_MARGIN' OR {{ ref('LkpReferences') }}.DELETED_TABLE_NAME = 'PL_MARGIN', 'Y', 'N') AS svJoinPart2,
		-- *SRC*: \(20)If (InModNullHandling.DELETED_TABLE_NAME = 'PL_FEE' Or InModNullHandling.DELETED_TABLE_NAME = 'PL_INT_RATE' Or InModNullHandling.DELETED_TABLE_NAME = 'PL_INT_RATE_AMT' Or InModNullHandling.DELETED_TABLE_NAME = 'PL_MARGIN') Then 'PL' Else  If (InModNullHandling.DELETED_TABLE_NAME = 'HL_FEATURE_ATTR' Or InModNullHandling.DELETED_TABLE_NAME = 'HL_FEE' Or InModNullHandling.DELETED_TABLE_NAME = 'HL_FEE_DISCOUNT' Or InModNullHandling.DELETED_TABLE_NAME = 'HL_INT_RATE' Or InModNullHandling.DELETED_TABLE_NAME = 'HL_INT_RATE_PERCENT' Or InModNullHandling.DELETED_TABLE_NAME = 'HL_PROD_IT_MARGIN') Then 'HL' Else  If (InModNullHandling.DELETED_TABLE_NAME = 'CCL_APP_FEE') Then 'CL' Else '',
		IFF(
	    {{ ref('LkpReferences') }}.DELETED_TABLE_NAME = 'PL_FEE' OR {{ ref('LkpReferences') }}.DELETED_TABLE_NAME = 'PL_INT_RATE' OR {{ ref('LkpReferences') }}.DELETED_TABLE_NAME = 'PL_INT_RATE_AMT' OR {{ ref('LkpReferences') }}.DELETED_TABLE_NAME = 'PL_MARGIN', 'PL',     
	    IFF(        
	        {{ ref('LkpReferences') }}.DELETED_TABLE_NAME = 'HL_FEATURE_ATTR'
	        or {{ ref('LkpReferences') }}.DELETED_TABLE_NAME = 'HL_FEE'
	        or {{ ref('LkpReferences') }}.DELETED_TABLE_NAME = 'HL_FEE_DISCOUNT'
	        or {{ ref('LkpReferences') }}.DELETED_TABLE_NAME = 'HL_INT_RATE'
	        or {{ ref('LkpReferences') }}.DELETED_TABLE_NAME = 'HL_INT_RATE_PERCENT'
	        or {{ ref('LkpReferences') }}.DELETED_TABLE_NAME = 'HL_PROD_IT_MARGIN', 
	        'HL', 
	        IFF({{ ref('LkpReferences') }}.DELETED_TABLE_NAME = 'CCL_APP_FEE', 'CL', '')
	    )
	) AS svStreamType1,
		-- *SRC*: \(20)If InModNullHandling.DELETED_TABLE_NAME = 'HL_APP_PROD' Then 'HL' Else  If InModNullHandling.DELETED_TABLE_NAME = 'PL_APP_PROD' Then 'PL' Else  If InModNullHandling.DELETED_TABLE_NAME = 'CCL_APP_PROD' Then 'CL' Else '',
		IFF({{ ref('LkpReferences') }}.DELETED_TABLE_NAME = 'HL_APP_PROD', 'HL', IFF({{ ref('LkpReferences') }}.DELETED_TABLE_NAME = 'PL_APP_PROD', 'PL', IFF({{ ref('LkpReferences') }}.DELETED_TABLE_NAME = 'CCL_APP_PROD', 'CL', ''))) AS svStreamType2,
		-- *SRC*: 'CSE' : 'PL' : InModNullHandling.DELETED_KEY_2_VALUE,
		CONCAT(CONCAT('CSE', 'PL'), {{ ref('LkpReferences') }}.DELETED_KEY_2_VALUE) AS APPT_PDCT_I,
		{{ ref('LkpReferences') }}.DELETED_KEY_1_VALUE AS SRCE_SYST_APPT_FEAT_I,
		-- *SRC*: DateFromDaysSince(-1, StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')) AS EXPY_D,
		{{ ref('LkpReferences') }}.PROS_KEY_I AS PROS_KEY_EXPY_I
	FROM {{ ref('LkpReferences') }}
	WHERE svLoadPLFEE = 'Y'
)

SELECT * FROM XfmTransform__PLFEE_OutApptPdctFeat