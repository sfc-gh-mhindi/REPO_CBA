{{ config(materialized='view', tags=['XfmCC_APP_PRODFrmExt']) }}

WITH XfmBusinessRules__OutErrorAcceptsCostsAndRisksDate AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((InXfmBusinessRules.SRCE_NUMC_1_C)) THEN (InXfmBusinessRules.SRCE_NUMC_1_C) ELSE ""))) = 0) Then DEFAULT_NULL_VALUE ELSE InXfmBusinessRules.TARG_CHAR_C,
		IFF(LEN(TRIM(IFF({{ ref('ModNullHandling') }}.SRCE_NUMC_1_C IS NOT NULL, {{ ref('ModNullHandling') }}.SRCE_NUMC_1_C, ''))) = 0, DEFAULT_NULL_VALUE, {{ ref('ModNullHandling') }}.TARG_CHAR_C) AS svFeatI,
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((InXfmBusinessRules.REQUESTED_LIMIT_AMT)) THEN (InXfmBusinessRules.REQUESTED_LIMIT_AMT) ELSE ""))) = 0) Then 'N' Else 'Y',
		IFF(LEN(TRIM(IFF({{ ref('ModNullHandling') }}.REQUESTED_LIMIT_AMT IS NOT NULL, {{ ref('ModNullHandling') }}.REQUESTED_LIMIT_AMT, ''))) = 0, 'N', 'Y') AS svReqLimitAmt,
		-- *SRC*: \(20)If IsNull(InXfmBusinessRules.PRE_APPRV_AMOUNT) Or Trim(( IF IsNotNull((InXfmBusinessRules.PRE_APPRV_AMOUNT)) THEN (InXfmBusinessRules.PRE_APPRV_AMOUNT) ELSE "")) = '' Then 'N' Else 'Y',
		IFF({{ ref('ModNullHandling') }}.PRE_APPRV_AMOUNT IS NULL OR TRIM(IFF({{ ref('ModNullHandling') }}.PRE_APPRV_AMOUNT IS NOT NULL, {{ ref('ModNullHandling') }}.PRE_APPRV_AMOUNT, '')) = '', 'N', 'Y') AS svPreApprvAmt,
		-- *SRC*: \(20)If IsNull(InXfmBusinessRules.CC_APP_PROD_ID) Or Trim(( IF IsNotNull((InXfmBusinessRules.CC_APP_PROD_ID)) THEN (InXfmBusinessRules.CC_APP_PROD_ID) ELSE "")) = '' Then 'N' Else 'Y',
		IFF({{ ref('ModNullHandling') }}.CC_APP_PROD_ID IS NULL OR TRIM(IFF({{ ref('ModNullHandling') }}.CC_APP_PROD_ID IS NOT NULL, {{ ref('ModNullHandling') }}.CC_APP_PROD_ID, '')) = '', 'N', 'Y') AS svCcAppProdId,
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((InXfmBusinessRules.CBA_HOMELOAN_NO)) THEN (InXfmBusinessRules.CBA_HOMELOAN_NO) ELSE ""))) = 0) Then 'N' Else 'Y',
		IFF(LEN(TRIM(IFF({{ ref('ModNullHandling') }}.CBA_HOMELOAN_NO IS NOT NULL, {{ ref('ModNullHandling') }}.CBA_HOMELOAN_NO, ''))) = 0, 'N', 'Y') AS svHomeLoan,
		-- *SRC*: \(20)If svFeatI = '999009' then 'RPR2101' else  if svFeatI = DEFAULT_NULL_VALUE then 'REJ2002' else '',
		IFF(svFeatI = '999009', 'RPR2101', IFF(svFeatI = DEFAULT_NULL_VALUE, 'REJ2002', '')) AS svErrorCode,
		-- *SRC*: \(20)If svErrorCode <> "" Then @TRUE Else @FALSE,
		IFF(svErrorCode <> '', @TRUE, @FALSE) AS svRejectFlag,
		-- *SRC*: \(20)If svFeatI = DEFAULT_NULL_VALUE then 'N' else 'Y',
		IFF(svFeatI = DEFAULT_NULL_VALUE, 'N', 'Y') AS svLoadFeatI,
		-- *SRC*: \(20)If Len(Trim(( IF IsNotNull((InXfmBusinessRules.CC_APP_PROD_ID)) THEN (InXfmBusinessRules.CC_APP_PROD_ID) ELSE ""))) = 0 Or Trim(InXfmBusinessRules.CC_APP_PROD_ID) = '0' Or Len(Trim(( IF IsNotNull((InXfmBusinessRules.READ_COSTS_AND_RISKS_FLAG)) THEN (InXfmBusinessRules.READ_COSTS_AND_RISKS_FLAG) ELSE ""))) = 0 Or Trim(( IF IsNotNull((InXfmBusinessRules.READ_COSTS_AND_RISKS_FLAG)) THEN (InXfmBusinessRules.READ_COSTS_AND_RISKS_FLAG) ELSE ('0'))) = '0' then 'N' else 'Y',
		IFF(    
	    LEN(TRIM(IFF({{ ref('ModNullHandling') }}.CC_APP_PROD_ID IS NOT NULL, {{ ref('ModNullHandling') }}.CC_APP_PROD_ID, ''))) = 0
	    or TRIM({{ ref('ModNullHandling') }}.CC_APP_PROD_ID) = '0'
	    or LEN(TRIM(IFF({{ ref('ModNullHandling') }}.READ_COSTS_AND_RISKS_FLAG IS NOT NULL, {{ ref('ModNullHandling') }}.READ_COSTS_AND_RISKS_FLAG, ''))) = 0
	    or TRIM(IFF({{ ref('ModNullHandling') }}.READ_COSTS_AND_RISKS_FLAG IS NOT NULL, {{ ref('ModNullHandling') }}.READ_COSTS_AND_RISKS_FLAG, '0')) = '0', 
	    'N', 
	    'Y'
	) AS svReadCosts,
		-- *SRC*: \(20)If Len(Trim(( IF IsNotNull((InXfmBusinessRules.CC_APP_PROD_ID)) THEN (InXfmBusinessRules.CC_APP_PROD_ID) ELSE ""))) = 0 Or Trim(InXfmBusinessRules.CC_APP_PROD_ID) = '0' Or Len(Trim(( IF IsNotNull((InXfmBusinessRules.ACCEPTS_COSTS_AND_RISKS_DATE)) THEN (InXfmBusinessRules.ACCEPTS_COSTS_AND_RISKS_DATE) ELSE ""))) = 0 Or Trim(( IF IsNotNull((InXfmBusinessRules.ACCEPTS_COSTS_AND_RISKS_DATE)) THEN (InXfmBusinessRules.ACCEPTS_COSTS_AND_RISKS_DATE) ELSE ('0'))) = '0' then 'N' else 'Y',
		IFF(    
	    LEN(TRIM(IFF({{ ref('ModNullHandling') }}.CC_APP_PROD_ID IS NOT NULL, {{ ref('ModNullHandling') }}.CC_APP_PROD_ID, ''))) = 0
	    or TRIM({{ ref('ModNullHandling') }}.CC_APP_PROD_ID) = '0'
	    or LEN(TRIM(IFF({{ ref('ModNullHandling') }}.ACCEPTS_COSTS_AND_RISKS_DATE IS NOT NULL, {{ ref('ModNullHandling') }}.ACCEPTS_COSTS_AND_RISKS_DATE, ''))) = 0
	    or TRIM(IFF({{ ref('ModNullHandling') }}.ACCEPTS_COSTS_AND_RISKS_DATE IS NOT NULL, {{ ref('ModNullHandling') }}.ACCEPTS_COSTS_AND_RISKS_DATE, '0')) = '0', 
	    'N', 
	    'Y'
	) AS svAcceptsCosts,
		-- *SRC*: \(20)if Len(Trim(( IF IsNotNull((InXfmBusinessRules.ACCEPTS_COSTS_AND_RISKS_DATE)) THEN (InXfmBusinessRules.ACCEPTS_COSTS_AND_RISKS_DATE) ELSE ""))) = 0 then '' else InXfmBusinessRules.ACCEPTS_COSTS_AND_RISKS_DATE[1, 4] : '-' : InXfmBusinessRules.ACCEPTS_COSTS_AND_RISKS_DATE[5, 2] : '-' : InXfmBusinessRules.ACCEPTS_COSTS_AND_RISKS_DATE[7, 2],
		IFF(
	    LEN(TRIM(IFF({{ ref('ModNullHandling') }}.ACCEPTS_COSTS_AND_RISKS_DATE IS NOT NULL, {{ ref('ModNullHandling') }}.ACCEPTS_COSTS_AND_RISKS_DATE, ''))) = 0, '', 
	    CONCAT(CONCAT(CONCAT(CONCAT(SUBSTRING({{ ref('ModNullHandling') }}.ACCEPTS_COSTS_AND_RISKS_DATE, 1, 4), '-'), SUBSTRING({{ ref('ModNullHandling') }}.ACCEPTS_COSTS_AND_RISKS_DATE, 5, 2)), '-'), SUBSTRING({{ ref('ModNullHandling') }}.ACCEPTS_COSTS_AND_RISKS_DATE, 7, 2))
	) AS svStusD,
		-- *SRC*: \(20)If IsValid('date', svStusD) Then StringToDate(InXfmBusinessRules.ACCEPTS_COSTS_AND_RISKS_DATE, '%yyyy%mm%dd') Else StringToDate(DEFAULT_DT, '%yyyy%mm%dd'),
		IFF(ISVALID('date', svStusD), STRINGTODATE({{ ref('ModNullHandling') }}.ACCEPTS_COSTS_AND_RISKS_DATE, '%yyyy%mm%dd'), STRINGTODATE(DEFAULT_DT, '%yyyy%mm%dd')) AS svStusd1,
		{{ ref('ModNullHandling') }}.CC_APP_PROD_ID AS SRCE_KEY_I,
		'ACCEPTS_COSTS_AND_RISKS_DATE' AS CONV_M,
		'' AS CONV_MAP_RULE_M,
		'APPT_PDCT_CHKL' AS TRSF_TABL_M,
		{{ ref('ModNullHandling') }}.ACCEPTS_COSTS_AND_RISKS_DATE AS VALU_CHNG_BFOR_X,
		svStusd1 AS VALU_CHNG_AFTR_X,
		-- *SRC*: 'Job Name = ' : DSJobName,
		CONCAT('Job Name = ', DSJobName) AS TRSF_X,
		'STUS_D' AS TRSF_COLM_M
	FROM {{ ref('ModNullHandling') }}
	WHERE svAcceptsCosts = 'Y' AND svStusd1 = DEFAULT_DT
)

SELECT * FROM XfmBusinessRules__OutErrorAcceptsCostsAndRisksDate