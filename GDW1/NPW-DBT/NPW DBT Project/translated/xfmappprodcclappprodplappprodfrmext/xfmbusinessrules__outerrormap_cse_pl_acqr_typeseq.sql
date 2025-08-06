{{ config(materialized='view', tags=['XfmAppProdCclAppProdPlAppProdFrmExt']) }}

WITH XfmBusinessRules__OutErrorMAP_CSE_PL_ACQR_TYPESeq AS (
	SELECT
		-- *SRC*: \(20)If ((InXfmBusinessRules.CPL_FOUND_FLAG = 'Y') or (InXfmBusinessRules.CC_FOUND_FLAG = 'Y') or (InXfmBusinessRules.CHL_FOUND_FLAG = 'Y')) then 'Y' else ( if Trim(( IF IsNotNull((InXfmBusinessRules.APPT_QLFY_C)) THEN (InXfmBusinessRules.APPT_QLFY_C) ELSE ('PO'))) = 'PO' then 'N' else 'Y'),
		IFF({{ ref('ModNullHandling') }}.CPL_FOUND_FLAG = 'Y' OR {{ ref('ModNullHandling') }}.CC_FOUND_FLAG = 'Y' OR {{ ref('ModNullHandling') }}.CHL_FOUND_FLAG = 'Y', 'Y', IFF(TRIM(IFF({{ ref('ModNullHandling') }}.APPT_QLFY_C IS NOT NULL, {{ ref('ModNullHandling') }}.APPT_QLFY_C, 'PO')) = 'PO', 'N', 'Y')) AS LoadF,
		-- *SRC*: \(20)If (InXfmBusinessRules.CPL_FOUND_FLAG = 'Y' and Len(Trim(( IF IsNotNull((InXfmBusinessRules.PL_TARG_CAT_ID)) THEN (InXfmBusinessRules.PL_TARG_CAT_ID) ELSE ""))) > 0) then InXfmBusinessRules.LOAN_FNDD_METH_C else DEFAULT_NULL_VALUE,
		IFF({{ ref('ModNullHandling') }}.CPL_FOUND_FLAG = 'Y' AND LEN(TRIM(IFF({{ ref('ModNullHandling') }}.PL_TARG_CAT_ID IS NOT NULL, {{ ref('ModNullHandling') }}.PL_TARG_CAT_ID, ''))) > 0, {{ ref('ModNullHandling') }}.LOAN_FNDD_METH_C, DEFAULT_NULL_VALUE) AS LoanFnddMethC,
		-- *SRC*: \(20)If (InXfmBusinessRules.CPL_FOUND_FLAG = 'Y' and Len(Trim(( IF IsNotNull((InXfmBusinessRules.PL_MRKT_SRCE_CAT_ID)) THEN (InXfmBusinessRules.PL_MRKT_SRCE_CAT_ID) ELSE ""))) > 0) then InXfmBusinessRules.ACQR_SRCE_C else DEFAULT_NULL_VALUE,
		IFF({{ ref('ModNullHandling') }}.CPL_FOUND_FLAG = 'Y' AND LEN(TRIM(IFF({{ ref('ModNullHandling') }}.PL_MRKT_SRCE_CAT_ID IS NOT NULL, {{ ref('ModNullHandling') }}.PL_MRKT_SRCE_CAT_ID, ''))) > 0, {{ ref('ModNullHandling') }}.ACQR_SRCE_C, DEFAULT_NULL_VALUE) AS AcqrSrceC,
		-- *SRC*: \(20)If (InXfmBusinessRules.CPL_FOUND_FLAG = 'Y' and Len(Trim(( IF IsNotNull((InXfmBusinessRules.PL_CMPN_CAT_ID)) THEN (InXfmBusinessRules.PL_CMPN_CAT_ID) ELSE ""))) > 0) then InXfmBusinessRules.ACQR_TYPE_C else DEFAULT_NULL_VALUE,
		IFF({{ ref('ModNullHandling') }}.CPL_FOUND_FLAG = 'Y' AND LEN(TRIM(IFF({{ ref('ModNullHandling') }}.PL_CMPN_CAT_ID IS NOT NULL, {{ ref('ModNullHandling') }}.PL_CMPN_CAT_ID, ''))) > 0, {{ ref('ModNullHandling') }}.ACQR_TYPE_C, DEFAULT_NULL_VALUE) AS AcqrTypeC,
		-- *SRC*: \(20)If (InXfmBusinessRules.CCL_FOUND_FLAG = 'Y') then 'CL' else ( if InXfmBusinessRules.CPL_FOUND_FLAG = 'Y' then 'PL' else ( if InXfmBusinessRules.CC_FOUND_FLAG = 'Y' then 'CC' else ( if InXfmBusinessRules.CHL_FOUND_FLAG = 'Y' then ( if InXfmBusinessRules.SBTY_CODE = 'HLM' then 'HM' else 'HL') else ( If LoadF = 'Y' then trim(( IF IsNotNull((InXfmBusinessRules.APPT_QLFY_C)) THEN (InXfmBusinessRules.APPT_QLFY_C) ELSE "")) else '')))),
		IFF(
	    {{ ref('ModNullHandling') }}.CCL_FOUND_FLAG = 'Y', 'CL',     
	    IFF(
	        {{ ref('ModNullHandling') }}.CPL_FOUND_FLAG = 'Y', 'PL', 
	        IFF({{ ref('ModNullHandling') }}.CC_FOUND_FLAG = 'Y', 'CC', IFF({{ ref('ModNullHandling') }}.CHL_FOUND_FLAG = 'Y', IFF({{ ref('ModNullHandling') }}.SBTY_CODE = 'HLM', 'HM', 'HL'), IFF(LoadF = 'Y', TRIM(IFF({{ ref('ModNullHandling') }}.APPT_QLFY_C IS NOT NULL, {{ ref('ModNullHandling') }}.APPT_QLFY_C, '')), '')))
	    )
	) AS ApptQlfyC,
		-- *SRC*: \(20)If AcqrSrceC[1, 4] = '9999' then 'RPR5106' else  if LoanFnddMethC[1, 4] = '9999' then 'RPR5107' else  if AcqrTypeC[1, 4] = '9999' then 'RPR5105' else  if Trim(ApptQlfyC) = '' then 'RPR5113' else '',
		IFF(SUBSTRING(AcqrSrceC, 1, 4) = '9999', 'RPR5106', IFF(SUBSTRING(LoanFnddMethC, 1, 4) = '9999', 'RPR5107', IFF(SUBSTRING(AcqrTypeC, 1, 4) = '9999', 'RPR5105', IFF(TRIM(ApptQlfyC) = '', 'RPR5113', '')))) AS ErrorCode,
		-- *SRC*: \(20)if InXfmBusinessRules.JOB_COMM_CATG_C = '9999' and Len(Trim(( IF IsNotNull((InXfmBusinessRules.CLP_JOB_FAMILY_CAT_ID)) THEN (InXfmBusinessRules.CLP_JOB_FAMILY_CAT_ID) ELSE ""))) = 0 then ( IF IsNotNull((InXfmBusinessRules.CLP_JOB_FAMILY_CAT_ID)) THEN (InXfmBusinessRules.CLP_JOB_FAMILY_CAT_ID) ELSE "") else InXfmBusinessRules.JOB_COMM_CATG_C,
		IFF(
	    {{ ref('ModNullHandling') }}.JOB_COMM_CATG_C = '9999' AND LEN(TRIM(IFF({{ ref('ModNullHandling') }}.CLP_JOB_FAMILY_CAT_ID IS NOT NULL, {{ ref('ModNullHandling') }}.CLP_JOB_FAMILY_CAT_ID, ''))) = 0, IFF({{ ref('ModNullHandling') }}.CLP_JOB_FAMILY_CAT_ID IS NOT NULL, {{ ref('ModNullHandling') }}.CLP_JOB_FAMILY_CAT_ID, ''), {{ ref('ModNullHandling') }}.JOB_COMM_CATG_C
	) AS LdCode,
		-- *SRC*: \(20)If ((InXfmBusinessRules.CPL_FOUND_FLAG = 'Y') or (InXfmBusinessRules.CC_FOUND_FLAG = 'Y') or (InXfmBusinessRules.CHL_FOUND_FLAG = 'Y')) then 'Y' else ( if Trim(( IF IsNotNull((InXfmBusinessRules.APPT_QLFY_C)) THEN (InXfmBusinessRules.APPT_QLFY_C) ELSE ('PO'))) = 'PO' then 'N' else 'Y'),
		IFF({{ ref('ModNullHandling') }}.CPL_FOUND_FLAG = 'Y' OR {{ ref('ModNullHandling') }}.CC_FOUND_FLAG = 'Y' OR {{ ref('ModNullHandling') }}.CHL_FOUND_FLAG = 'Y', 'Y', IFF(TRIM(IFF({{ ref('ModNullHandling') }}.APPT_QLFY_C IS NOT NULL, {{ ref('ModNullHandling') }}.APPT_QLFY_C, 'PO')) = 'PO', 'N', 'Y')) AS LdNwF,
		-- *SRC*: \(20)If (Trim(( IF IsNotNull((InXfmBusinessRules.SBTY_CODE)) THEN (InXfmBusinessRules.SBTY_CODE) ELSE "")) = 'PL' or Trim(( IF IsNotNull((InXfmBusinessRules.SBTY_CODE)) THEN (InXfmBusinessRules.SBTY_CODE) ELSE "")) = 'CC') then 'Y' else ( If (Trim(( IF IsNotNull((InXfmBusinessRules.SBTY_CODE)) THEN (InXfmBusinessRules.SBTY_CODE) ELSE "")) = 'CL' OR (InXfmBusinessRules.CCL_FOUND_FLAG = 'Y')) then InXfmBusinessRules.CCL_NEW_PROD_FLAG else ' '),
		IFF(
	    TRIM(IFF({{ ref('ModNullHandling') }}.SBTY_CODE IS NOT NULL, {{ ref('ModNullHandling') }}.SBTY_CODE, '')) = 'PL' OR TRIM(IFF({{ ref('ModNullHandling') }}.SBTY_CODE IS NOT NULL, {{ ref('ModNullHandling') }}.SBTY_CODE, '')) = 'CC', 'Y', 
	    IFF(TRIM(IFF({{ ref('ModNullHandling') }}.SBTY_CODE IS NOT NULL, {{ ref('ModNullHandling') }}.SBTY_CODE, '')) = 'CL'
	    or {{ ref('ModNullHandling') }}.CCL_FOUND_FLAG = 'Y', {{ ref('ModNullHandling') }}.CCL_NEW_PROD_FLAG, ' ')
	) AS LdNewFlg,
		-- *SRC*: \(20)if (Trim(( IF IsNotNull((InXfmBusinessRules.CLP_FOUND_FLAG)) THEN (InXfmBusinessRules.CLP_FOUND_FLAG) ELSE ""))) = 'Y' THEN 'N' ELSE LdNwF,
		IFF(TRIM(IFF({{ ref('ModNullHandling') }}.CLP_FOUND_FLAG IS NOT NULL, {{ ref('ModNullHandling') }}.CLP_FOUND_FLAG, '')) = 'Y', 'N', LdNwF) AS LdNewFlag,
		-- *SRC*: \(20)If IsNull(InXfmBusinessRules.ABN) OR Trim(( IF IsNotNull((InXfmBusinessRules.ABN)) THEN (InXfmBusinessRules.ABN) ELSE "")) = '' THEN 'N' ELSE 'Y',
		IFF({{ ref('ModNullHandling') }}.ABN IS NULL OR TRIM(IFF({{ ref('ModNullHandling') }}.ABN IS NOT NULL, {{ ref('ModNullHandling') }}.ABN, '')) = '', 'N', 'Y') AS svIsNullAbn,
		-- *SRC*: \(20)If IsNull(InXfmBusinessRules.BUSINESS_NAME) OR Trim(( IF IsNotNull((InXfmBusinessRules.BUSINESS_NAME)) THEN (InXfmBusinessRules.BUSINESS_NAME) ELSE "")) = '' THEN 'N' ELSE 'Y',
		IFF({{ ref('ModNullHandling') }}.BUSINESS_NAME IS NULL OR TRIM(IFF({{ ref('ModNullHandling') }}.BUSINESS_NAME IS NOT NULL, {{ ref('ModNullHandling') }}.BUSINESS_NAME, '')) = '', 'N', 'Y') AS svIsNullBusinessName,
		-- *SRC*: \(20)If IsNull(InXfmBusinessRules.SIMPLE_APP_FLAG) OR Trim(( IF IsNotNull((InXfmBusinessRules.SIMPLE_APP_FLAG)) THEN (InXfmBusinessRules.SIMPLE_APP_FLAG) ELSE "")) = '' THEN 'N' ELSE 'Y',
		IFF({{ ref('ModNullHandling') }}.SIMPLE_APP_FLAG IS NULL OR TRIM(IFF({{ ref('ModNullHandling') }}.SIMPLE_APP_FLAG IS NOT NULL, {{ ref('ModNullHandling') }}.SIMPLE_APP_FLAG, '')) = '', 'N', 'Y') AS svIsNullSmplFlg,
		-- *SRC*: \(20)if Trim(( IF IsNotNull((InXfmBusinessRules.CHL_ASSESSMENT_DATE)) THEN (InXfmBusinessRules.CHL_ASSESSMENT_DATE) ELSE "")) = '' then 'Y' else InXfmBusinessRules.CHL_ASSESSMENT_DATE[1, 4] : '-' : InXfmBusinessRules.CHL_ASSESSMENT_DATE[5, 2] : '-' : InXfmBusinessRules.CHL_ASSESSMENT_DATE[7, 2],
		IFF(
	    TRIM(IFF({{ ref('ModNullHandling') }}.CHL_ASSESSMENT_DATE IS NOT NULL, {{ ref('ModNullHandling') }}.CHL_ASSESSMENT_DATE, '')) = '', 'Y', 
	    CONCAT(CONCAT(CONCAT(CONCAT(SUBSTRING({{ ref('ModNullHandling') }}.CHL_ASSESSMENT_DATE, 1, 4), '-'), SUBSTRING({{ ref('ModNullHandling') }}.CHL_ASSESSMENT_DATE, 5, 2)), '-'), SUBSTRING({{ ref('ModNullHandling') }}.CHL_ASSESSMENT_DATE, 7, 2))
	) AS svPreAssDt,
		-- *SRC*: \(20)If svPreAssDt = 'Y' Then '' Else  If IsValid('date', svPreAssDt) Then svPreAssDt Else StringToDate(DEFAULT_DT, '%yyyy%mm%dd'),
		IFF(svPreAssDt = 'Y', '', IFF(ISVALID('date', svPreAssDt), svPreAssDt, STRINGTODATE(DEFAULT_DT, '%yyyy%mm%dd'))) AS svAssessmentDt,
		{{ ref('ModNullHandling') }}.APP_PROD_ID AS SRCE_KEY_I,
		'PL_CMPN_CAT_ID' AS CONV_M,
		'LOOKUP' AS CONV_MAP_RULE_M,
		'MAP_CSE_PL_ACQR_TYPE' AS TRSF_TABL_M,
		{{ ref('ModNullHandling') }}.PL_CMPN_CAT_ID AS VALU_CHNG_BFOR_X,
		AcqrTypeC AS VALU_CHNG_AFTR_X,
		-- *SRC*: 'Job Name = ' : DSJobName,
		CONCAT('Job Name = ', DSJobName) AS TRSF_X,
		'ACQR_TYPE_C' AS TRSF_COLM_M
	FROM {{ ref('ModNullHandling') }}
	WHERE LoadF = 'Y' AND SUBSTRING(AcqrTypeC, 1, 4) = '9999'
)

SELECT * FROM XfmBusinessRules__OutErrorMAP_CSE_PL_ACQR_TYPESeq