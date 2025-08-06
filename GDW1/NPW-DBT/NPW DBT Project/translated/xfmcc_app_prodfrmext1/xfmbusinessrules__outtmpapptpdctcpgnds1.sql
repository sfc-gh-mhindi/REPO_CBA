{{ config(materialized='view', tags=['XfmCC_APP_PRODFrmExt1']) }}

WITH XfmBusinessRules__OutTmpApptPdctCpgnDs1 AS (
	SELECT
		-- *SRC*: \(20)If Trim(( IF IsNotNull((InXfmBusinessRules.CC_APP_PROD_ID)) THEN (InXfmBusinessRules.CC_APP_PROD_ID) ELSE "")) = '' Or Trim(( IF IsNotNull((InXfmBusinessRules.CC_APP_PROD_ID)) THEN (InXfmBusinessRules.CC_APP_PROD_ID) ELSE ""), '0', 'L') = '' Or Trim(( IF IsNotNull((InXfmBusinessRules.NTM_CAMPAIGN_ID)) THEN (InXfmBusinessRules.NTM_CAMPAIGN_ID) ELSE "")) = '' Or Trim(( IF IsNotNull((InXfmBusinessRules.NTM_CAMPAIGN_ID)) THEN (InXfmBusinessRules.NTM_CAMPAIGN_ID) ELSE ""), '0', 'L') = '' Then 'N' Else 'Y',
		IFF(    
	    TRIM(IFF({{ ref('SrcCCAppProdPremapDS') }}.CC_APP_PROD_ID IS NOT NULL, {{ ref('SrcCCAppProdPremapDS') }}.CC_APP_PROD_ID, '')) = ''
	    or TRIM(IFF({{ ref('SrcCCAppProdPremapDS') }}.CC_APP_PROD_ID IS NOT NULL, {{ ref('SrcCCAppProdPremapDS') }}.CC_APP_PROD_ID, ''), '0', 'L') = ''
	    or TRIM(IFF({{ ref('SrcCCAppProdPremapDS') }}.NTM_CAMPAIGN_ID IS NOT NULL, {{ ref('SrcCCAppProdPremapDS') }}.NTM_CAMPAIGN_ID, '')) = ''
	    or TRIM(IFF({{ ref('SrcCCAppProdPremapDS') }}.NTM_CAMPAIGN_ID IS NOT NULL, {{ ref('SrcCCAppProdPremapDS') }}.NTM_CAMPAIGN_ID, ''), '0', 'L') = '', 
	    'N', 
	    'Y'
	) AS svDdm,
		-- *SRC*: \(20)If Trim(( IF IsNotNull((InXfmBusinessRules.CC_APP_PROD_ID)) THEN (InXfmBusinessRules.CC_APP_PROD_ID) ELSE "")) = '' Or Trim(( IF IsNotNull((InXfmBusinessRules.CC_APP_PROD_ID)) THEN (InXfmBusinessRules.CC_APP_PROD_ID) ELSE ""), '0', 'L') = '' Or Trim(( IF IsNotNull((InXfmBusinessRules.FRIES_CAMPAIGN_CODE)) THEN (InXfmBusinessRules.FRIES_CAMPAIGN_CODE) ELSE "")) = '' Or Trim(( IF IsNotNull((InXfmBusinessRules.FRIES_CAMPAIGN_CODE)) THEN (InXfmBusinessRules.FRIES_CAMPAIGN_CODE) ELSE ""), '0', 'L') = '' Then 'N' Else 'Y',
		IFF(    
	    TRIM(IFF({{ ref('SrcCCAppProdPremapDS') }}.CC_APP_PROD_ID IS NOT NULL, {{ ref('SrcCCAppProdPremapDS') }}.CC_APP_PROD_ID, '')) = ''
	    or TRIM(IFF({{ ref('SrcCCAppProdPremapDS') }}.CC_APP_PROD_ID IS NOT NULL, {{ ref('SrcCCAppProdPremapDS') }}.CC_APP_PROD_ID, ''), '0', 'L') = ''
	    or TRIM(IFF({{ ref('SrcCCAppProdPremapDS') }}.FRIES_CAMPAIGN_CODE IS NOT NULL, {{ ref('SrcCCAppProdPremapDS') }}.FRIES_CAMPAIGN_CODE, '')) = ''
	    or TRIM(IFF({{ ref('SrcCCAppProdPremapDS') }}.FRIES_CAMPAIGN_CODE IS NOT NULL, {{ ref('SrcCCAppProdPremapDS') }}.FRIES_CAMPAIGN_CODE, ''), '0', 'L') = '', 
	    'N', 
	    'Y'
	) AS svCsc,
		-- *SRC*: \(20)If Trim(( IF IsNotNull((InXfmBusinessRules.CC_APP_PROD_ID)) THEN (InXfmBusinessRules.CC_APP_PROD_ID) ELSE "")) = '' Or Trim(( IF IsNotNull((InXfmBusinessRules.CC_APP_PROD_ID)) THEN (InXfmBusinessRules.CC_APP_PROD_ID) ELSE ""), '0', 'L') = '' Or Trim(( IF IsNotNull((InXfmBusinessRules.OAP_CAMPAIGN_CODE)) THEN (InXfmBusinessRules.OAP_CAMPAIGN_CODE) ELSE "")) = '' Or Trim(( IF IsNotNull((InXfmBusinessRules.OAP_CAMPAIGN_CODE)) THEN (InXfmBusinessRules.OAP_CAMPAIGN_CODE) ELSE ""), '0', 'L') = '' Then 'N' Else 'Y',
		IFF(    
	    TRIM(IFF({{ ref('SrcCCAppProdPremapDS') }}.CC_APP_PROD_ID IS NOT NULL, {{ ref('SrcCCAppProdPremapDS') }}.CC_APP_PROD_ID, '')) = ''
	    or TRIM(IFF({{ ref('SrcCCAppProdPremapDS') }}.CC_APP_PROD_ID IS NOT NULL, {{ ref('SrcCCAppProdPremapDS') }}.CC_APP_PROD_ID, ''), '0', 'L') = ''
	    or TRIM(IFF({{ ref('SrcCCAppProdPremapDS') }}.OAP_CAMPAIGN_CODE IS NOT NULL, {{ ref('SrcCCAppProdPremapDS') }}.OAP_CAMPAIGN_CODE, '')) = ''
	    or TRIM(IFF({{ ref('SrcCCAppProdPremapDS') }}.OAP_CAMPAIGN_CODE IS NOT NULL, {{ ref('SrcCCAppProdPremapDS') }}.OAP_CAMPAIGN_CODE, ''), '0', 'L') = '', 
	    'N', 
	    'Y'
	) AS svGcsc,
		-- *SRC*: 'CSECC' : InXfmBusinessRules.CC_APP_PROD_ID,
		CONCAT('CSECC', {{ ref('SrcCCAppProdPremapDS') }}.CC_APP_PROD_ID) AS APPT_PDCT_I,
		'DDM' AS CPGN_TYPE_C,
		-- *SRC*: Trim(( IF IsNotNull((InXfmBusinessRules.NTM_CAMPAIGN_ID)) THEN (InXfmBusinessRules.NTM_CAMPAIGN_ID) ELSE "")),
		TRIM(IFF({{ ref('SrcCCAppProdPremapDS') }}.NTM_CAMPAIGN_ID IS NOT NULL, {{ ref('SrcCCAppProdPremapDS') }}.NTM_CAMPAIGN_ID, '')) AS CPGN_I,
		'ORIG' AS REL_C,
		'CSE' AS SRCE_SYST_C,
		-- *SRC*: StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd'),
		STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd') AS EFFT_D,
		-- *SRC*: StringToDate('99991231', '%yyyy%mm%dd'),
		STRINGTODATE('99991231', '%yyyy%mm%dd') AS EXPY_D,
		RUN_STREAM AS RUN_STRM
	FROM {{ ref('SrcCCAppProdPremapDS') }}
	WHERE svDdm = 'Y'
)

SELECT * FROM XfmBusinessRules__OutTmpApptPdctCpgnDs1