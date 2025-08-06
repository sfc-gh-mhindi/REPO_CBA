{{ config(materialized='view', tags=['XfmCclAppProdFrmExt3']) }}

WITH XfmBusinessRules__ApptPdctRel2 AS (
	SELECT
		-- *SRC*: \(20)If Len(Trim(( IF IsNotNull((InXfmBusinessRules.PARENT_CCL_APP_PROD_ID)) THEN (InXfmBusinessRules.PARENT_CCL_APP_PROD_ID) ELSE ""))) = 0 Then 'N' Else 'Y',
		IFF(LEN(TRIM(IFF({{ ref('ModNullHandling') }}.PARENT_CCL_APP_PROD_ID IS NOT NULL, {{ ref('ModNullHandling') }}.PARENT_CCL_APP_PROD_ID, ''))) = 0, 'N', 'Y') AS LoadApptPdctRel1,
		-- *SRC*: \(20)If Len(Trim(( IF IsNotNull((InXfmBusinessRules.LINK_CCL_APP_PROD_ID)) THEN (InXfmBusinessRules.LINK_CCL_APP_PROD_ID) ELSE ""))) = 0 Then 'N' Else 'Y',
		IFF(LEN(TRIM(IFF({{ ref('ModNullHandling') }}.LINK_CCL_APP_PROD_ID IS NOT NULL, {{ ref('ModNullHandling') }}.LINK_CCL_APP_PROD_ID, ''))) = 0, 'N', 'Y') AS LoadApptPdctRel2,
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((InXfmBusinessRules.CRIS_PRODUCT_ID)) THEN (InXfmBusinessRules.CRIS_PRODUCT_ID) ELSE ""))) = 0 Or Len(Trim(( IF IsNotNull((InXfmBusinessRules.ACCT_I_PRFX)) THEN (InXfmBusinessRules.ACCT_I_PRFX) ELSE ""))) = 0 Or Len(Trim(( IF IsNotNull((InXfmBusinessRules.ACCOUNT_NUMBER)) THEN (InXfmBusinessRules.ACCOUNT_NUMBER) ELSE ""))) = 0) Then 'N' Else 'Y',
		IFF(    
	    LEN(TRIM(IFF({{ ref('ModNullHandling') }}.CRIS_PRODUCT_ID IS NOT NULL, {{ ref('ModNullHandling') }}.CRIS_PRODUCT_ID, ''))) = 0
	    or LEN(TRIM(IFF({{ ref('ModNullHandling') }}.ACCT_I_PRFX IS NOT NULL, {{ ref('ModNullHandling') }}.ACCT_I_PRFX, ''))) = 0
	    or LEN(TRIM(IFF({{ ref('ModNullHandling') }}.ACCOUNT_NUMBER IS NOT NULL, {{ ref('ModNullHandling') }}.ACCOUNT_NUMBER, ''))) = 0, 
	    'N', 
	    'Y'
	) AS LoadApptPdctAcct1,
		-- *SRC*: \(20)If Len(Trim(( IF IsNotNull((InXfmBusinessRules.BALLOON_PAY_AMT)) THEN (InXfmBusinessRules.BALLOON_PAY_AMT) ELSE ""))) = 0 Then 'N' Else 'Y',
		IFF(LEN(TRIM(IFF({{ ref('ModNullHandling') }}.BALLOON_PAY_AMT IS NOT NULL, {{ ref('ModNullHandling') }}.BALLOON_PAY_AMT, ''))) = 0, 'N', 'Y') AS LoadApptPdctAmt1,
		-- *SRC*: \(20)If Len(Trim(( IF IsNotNull((InXfmBusinessRules.CURRENT_LIMIT)) THEN (InXfmBusinessRules.CURRENT_LIMIT) ELSE ""))) = 0 Then 'N' Else 'Y',
		IFF(LEN(TRIM(IFF({{ ref('ModNullHandling') }}.CURRENT_LIMIT IS NOT NULL, {{ ref('ModNullHandling') }}.CURRENT_LIMIT, ''))) = 0, 'N', 'Y') AS LoadApptPdctAmt2,
		-- *SRC*: \(20)If Len(Trim(( IF IsNotNull((InXfmBusinessRules.INC_DEC_AMT)) THEN (InXfmBusinessRules.INC_DEC_AMT) ELSE ""))) = 0 Then 'N' Else 'Y',
		IFF(LEN(TRIM(IFF({{ ref('ModNullHandling') }}.INC_DEC_AMT IS NOT NULL, {{ ref('ModNullHandling') }}.INC_DEC_AMT, ''))) = 0, 'N', 'Y') AS LoadApptPdctAmt3,
		-- *SRC*: \(20)If Len(Trim(( IF IsNotNull((InXfmBusinessRules.AD_TUC_INC_AMT)) THEN (InXfmBusinessRules.AD_TUC_INC_AMT) ELSE ""))) = 0 Then 'N' Else 'Y',
		IFF(LEN(TRIM(IFF({{ ref('ModNullHandling') }}.AD_TUC_INC_AMT IS NOT NULL, {{ ref('ModNullHandling') }}.AD_TUC_INC_AMT, ''))) = 0, 'N', 'Y') AS LoadApptPdctAmt4,
		-- *SRC*: \(20)If Len(Trim(( IF IsNotNull((InXfmBusinessRules.CCL_LOAN_PURPOSE_ID)) THEN (InXfmBusinessRules.CCL_LOAN_PURPOSE_ID) ELSE ""))) = 0 And Len(Trim(( IF IsNotNull((InXfmBusinessRules.LOAN_PURPOSE_CLASS_CODE)) THEN (InXfmBusinessRules.LOAN_PURPOSE_CLASS_CODE) ELSE ""))) = 0 Then 'N' Else 'Y',
		IFF(LEN(TRIM(IFF({{ ref('ModNullHandling') }}.CCL_LOAN_PURPOSE_ID IS NOT NULL, {{ ref('ModNullHandling') }}.CCL_LOAN_PURPOSE_ID, ''))) = 0 AND LEN(TRIM(IFF({{ ref('ModNullHandling') }}.LOAN_PURPOSE_CLASS_CODE IS NOT NULL, {{ ref('ModNullHandling') }}.LOAN_PURPOSE_CLASS_CODE, ''))) = 0, 'N', 'Y') AS LoadApptPdctPurp,
		-- *SRC*: \(20)If Len(Trim(( IF IsNotNull((InXfmBusinessRules.LINK_CCL_APP_ID)) THEN (InXfmBusinessRules.LINK_CCL_APP_ID) ELSE ""))) = 0 Then 'N' Else 'Y',
		IFF(LEN(TRIM(IFF({{ ref('ModNullHandling') }}.LINK_CCL_APP_ID IS NOT NULL, {{ ref('ModNullHandling') }}.LINK_CCL_APP_ID, ''))) = 0, 'N', 'Y') AS LoadApptRel1,
		-- *SRC*: \(20)If Len(Trim(( IF IsNotNull((InXfmBusinessRules.CCL_APP_BROKER_CIF_CODE)) THEN (InXfmBusinessRules.CCL_APP_BROKER_CIF_CODE) ELSE ""))) = 0 Then 'N' Else 'Y',
		IFF(LEN(TRIM(IFF({{ ref('ModNullHandling') }}.CCL_APP_BROKER_CIF_CODE IS NOT NULL, {{ ref('ModNullHandling') }}.CCL_APP_BROKER_CIF_CODE, ''))) = 0, 'N', 'Y') AS LoadApptPdctPaty1,
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((InXfmBusinessRules.PARENT_PRODUCT_LEVEL_CAT_ID)) THEN (InXfmBusinessRules.PARENT_PRODUCT_LEVEL_CAT_ID) ELSE ""))) = 0) Or (Len(Trim(( IF IsNotNull((InXfmBusinessRules.CHILD_PRODUCT_LEVEL_CAT_ID)) THEN (InXfmBusinessRules.CHILD_PRODUCT_LEVEL_CAT_ID) ELSE ""))) = 0) Then 'UNKN' Else InXfmBusinessRules.REL_C,
		IFF(LEN(TRIM(IFF({{ ref('ModNullHandling') }}.PARENT_PRODUCT_LEVEL_CAT_ID IS NOT NULL, {{ ref('ModNullHandling') }}.PARENT_PRODUCT_LEVEL_CAT_ID, ''))) = 0 OR LEN(TRIM(IFF({{ ref('ModNullHandling') }}.CHILD_PRODUCT_LEVEL_CAT_ID IS NOT NULL, {{ ref('ModNullHandling') }}.CHILD_PRODUCT_LEVEL_CAT_ID, ''))) = 0, 'UNKN', {{ ref('ModNullHandling') }}.REL_C) AS RelTypeC,
		-- *SRC*: \(20)If Len(Trim(( IF IsNotNull((InXfmBusinessRules.CCL_LOAN_PURPOSE_ID)) THEN (InXfmBusinessRules.CCL_LOAN_PURPOSE_ID) ELSE ""))) = 0 Then 'UNKN' Else InXfmBusinessRules.PURP_TYPE_C,
		IFF(LEN(TRIM(IFF({{ ref('ModNullHandling') }}.CCL_LOAN_PURPOSE_ID IS NOT NULL, {{ ref('ModNullHandling') }}.CCL_LOAN_PURPOSE_ID, ''))) = 0, 'UNKN', {{ ref('ModNullHandling') }}.PURP_TYPE_C) AS PurpTypeC,
		-- *SRC*: \(20)If Len(Trim(( IF IsNotNull((InXfmBusinessRules.LOAN_PURPOSE_CLASS_CODE)) THEN (InXfmBusinessRules.LOAN_PURPOSE_CLASS_CODE) ELSE ""))) = 0 Then 'UNKN' Else InXfmBusinessRules.PURP_CLAS_C,
		IFF(LEN(TRIM(IFF({{ ref('ModNullHandling') }}.LOAN_PURPOSE_CLASS_CODE IS NOT NULL, {{ ref('ModNullHandling') }}.LOAN_PURPOSE_CLASS_CODE, ''))) = 0, 'UNKN', {{ ref('ModNullHandling') }}.PURP_CLAS_C) AS PurpClassC,
		-- *SRC*: \(20)If (InXfmBusinessRules.REL_C = '99999' And LoadApptPdctRel1 = 'Y') Then 'RPR3110' Else '',
		IFF({{ ref('ModNullHandling') }}.REL_C = '99999' AND LoadApptPdctRel1 = 'Y', 'RPR3110', '') AS ErrorCode_1,
		-- *SRC*: \(20)If (PurpTypeC = '9999') Then 'RPR3106' Else '',
		IFF(PurpTypeC = '9999', 'RPR3106', '') AS ErrorCode_2,
		-- *SRC*: \(20)If (PurpClassC = '9999') Then 'RPR3107' Else '',
		IFF(PurpClassC = '9999', 'RPR3107', '') AS ErrorCode_3,
		-- *SRC*: \(20)If ErrorCode_1 <> "" Then ErrorCode_1 Else  If ErrorCode_2 <> "" Then ErrorCode_2 Else  If ErrorCode_3 <> "" Then ErrorCode_3 Else "",
		IFF(ErrorCode_1 <> '', ErrorCode_1, IFF(ErrorCode_2 <> '', ErrorCode_2, IFF(ErrorCode_3 <> '', ErrorCode_3, ''))) AS ErrorCode,
		-- *SRC*: \(20)If ErrorCode <> "" Then @TRUE Else @FALSE,
		IFF(ErrorCode <> '', @TRUE, @FALSE) AS RejectFlag,
		-- *SRC*: \(20)If Len(Trim(( IF IsNotNull((InXfmBusinessRules.ACCOUNT_NUMBER)) THEN (InXfmBusinessRules.ACCOUNT_NUMBER) ELSE ""))) = 0 Then 'N' Else 'Y',
		IFF(LEN(TRIM(IFF({{ ref('ModNullHandling') }}.ACCOUNT_NUMBER IS NOT NULL, {{ ref('ModNullHandling') }}.ACCOUNT_NUMBER, ''))) = 0, 'N', 'Y') AS LoadApptPdctAcct2,
		-- *SRC*: 'CSECL' : InXfmBusinessRules.CCL_APP_PROD_ID,
		CONCAT('CSECL', {{ ref('ModNullHandling') }}.CCL_APP_PROD_ID) AS APPT_PDCT_I,
		-- *SRC*: 'CSECL' : InXfmBusinessRules.LINK_CCL_APP_PROD_ID,
		CONCAT('CSECL', {{ ref('ModNullHandling') }}.LINK_CCL_APP_PROD_ID) AS RELD_APPT_PDCT_I,
		-- *SRC*: StringToDate(ETL_PROCESS_DT, "%yyyy%mm%dd"),
		STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd') AS EFFT_D,
		'CXPA' AS REL_TYPE_C,
		-- *SRC*: StringToDate('9999-12-31', "%yyyy-%mm-%dd"),
		STRINGTODATE('9999-12-31', '%yyyy-%mm-%dd') AS EXPY_D,
		0 AS PROS_KEY_EFFT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS PROS_KEY_EXPY_I,
		-- *SRC*: SetNull(),
		SETNULL() AS EROR_SEQN_I,
		RUN_STREAM AS RUN_STRM
	FROM {{ ref('ModNullHandling') }}
	WHERE LoadApptPdctRel2 = 'Y'
)

SELECT * FROM XfmBusinessRules__ApptPdctRel2