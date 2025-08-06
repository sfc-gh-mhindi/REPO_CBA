{{ config(materialized='view', tags=['XfmAppCclAppChlAppFrmExt']) }}

WITH XfmBusinessRules__OutErrorRecvDate AS (
	SELECT
		-- *SRC*: \(20)If (InXfmBusinessRules.APP_FOUND_FLAG = 'N' And InXfmBusinessRules.CCL_APP_FOUND_FLAG = 'Y') Then 'CL' Else  If (InXfmBusinessRules.APP_FOUND_FLAG = 'N' And InXfmBusinessRules.CHL_APP_FOUND_FLAG = 'Y') Then 'HL' Else  If (InXfmBusinessRules.APP_FOUND_FLAG = 'N' And InXfmBusinessRules.CCC_APP_FOUND_FLAG = 'Y') Then 'CC' Else  If (InXfmBusinessRules.APP_FOUND_FLAG = 'N' And ( IF IsNotNull((InXfmBusinessRules.CHL_TPB_FOUND_FLAG)) THEN (InXfmBusinessRules.CHL_TPB_FOUND_FLAG) ELSE "") = 'Y') then InXfmBusinessRules.TPB_APPT_QLFY_C else InXfmBusinessRules.APPT_QLFY_C,
		IFF(
	    {{ ref('ModNullHandling') }}.APP_FOUND_FLAG = 'N' AND {{ ref('ModNullHandling') }}.CCL_APP_FOUND_FLAG = 'Y', 'CL',     
	    IFF(
	        {{ ref('ModNullHandling') }}.APP_FOUND_FLAG = 'N'
	    and {{ ref('ModNullHandling') }}.CHL_APP_FOUND_FLAG = 'Y', 'HL',         
	        IFF(
	            {{ ref('ModNullHandling') }}.APP_FOUND_FLAG = 'N'
	        and {{ ref('ModNullHandling') }}.CCC_APP_FOUND_FLAG = 'Y', 'CC', 
	            IFF({{ ref('ModNullHandling') }}.APP_FOUND_FLAG = 'N'
	            and IFF({{ ref('ModNullHandling') }}.CHL_TPB_FOUND_FLAG IS NOT NULL, {{ ref('ModNullHandling') }}.CHL_TPB_FOUND_FLAG, '') = 'Y', {{ ref('ModNullHandling') }}.TPB_APPT_QLFY_C, {{ ref('ModNullHandling') }}.APPT_QLFY_C)
	        )
	    )
	) AS ApptQlfyC,
		-- *SRC*: \(20)If trim(ApptQlfyC) = '99' then @FALSE Else @TRUE,
		IFF(TRIM(ApptQlfyC) = '99', @FALSE, @TRUE) AS LoadApptQlfyC,
		-- *SRC*: \(20)If ApptQlfyC <> 'CL' Or Len(Trim(( IF IsNotNull((InXfmBusinessRules.CCL_APP_CCL_APP_CAT_ID)) THEN (InXfmBusinessRules.CCL_APP_CCL_APP_CAT_ID) ELSE ""))) = 0 Then DEFAULT_NULL_VALUE Else trim(InXfmBusinessRules.APPT_C),
		IFF(ApptQlfyC <> 'CL' OR LEN(TRIM(IFF({{ ref('ModNullHandling') }}.CCL_APP_CCL_APP_CAT_ID IS NOT NULL, {{ ref('ModNullHandling') }}.CCL_APP_CCL_APP_CAT_ID, ''))) = 0, DEFAULT_NULL_VALUE, TRIM({{ ref('ModNullHandling') }}.APPT_C)) AS ApptC,
		-- *SRC*: \(20)If trim(( IF IsNotNull((InXfmBusinessRules.APP_SUBTYPE_CODE)) THEN (InXfmBusinessRules.APP_SUBTYPE_CODE) ELSE "")) = 'TU' Then trim(InXfmBusinessRules.APPT_TU_C) else  if Trim(( IF IsNotNull((InXfmBusinessRules.CHL_TPB_SUBTYPE_CODE)) THEN (InXfmBusinessRules.CHL_TPB_SUBTYPE_CODE) ELSE "")) = 'TU' then InXfmBusinessRules.APPT_C_TPB Else DEFAULT_NULL_VALUE,
		IFF(
	    TRIM(IFF({{ ref('ModNullHandling') }}.APP_SUBTYPE_CODE IS NOT NULL, {{ ref('ModNullHandling') }}.APP_SUBTYPE_CODE, '')) = 'TU', TRIM({{ ref('ModNullHandling') }}.APPT_TU_C), 
	    IFF(TRIM(IFF({{ ref('ModNullHandling') }}.CHL_TPB_SUBTYPE_CODE IS NOT NULL, {{ ref('ModNullHandling') }}.CHL_TPB_SUBTYPE_CODE, '')) = 'TU', {{ ref('ModNullHandling') }}.APPT_C_TPB, DEFAULT_NULL_VALUE)
	) AS TuApptC,
		-- *SRC*: \(20)If (IsNotNull(InXfmBusinessRules.CCC_APP_CC_APP_CAT_ID) And InXfmBusinessRules.CCC_APP_CC_APP_CAT_ID = '1' And ApptQlfyC = 'CC') Then 'PSSF' Else  If (IsNotNull(InXfmBusinessRules.CCC_APP_CC_APP_CAT_ID) And InXfmBusinessRules.CCC_APP_CC_APP_CAT_ID = '2' And ApptQlfyC = 'CC') Then 'PASF' Else  If (IsNotNull(InXfmBusinessRules.CCC_APP_CC_APP_CAT_ID) And InXfmBusinessRules.CCC_APP_CC_APP_CAT_ID = '3' And ApptQlfyC = 'CC') Then 'FAFF' Else  If (ApptQlfyC <> 'CL' Or Len(Trim(( IF IsNotNull((InXfmBusinessRules.CCL_APP_CCL_FORM_CAT_ID)) THEN (InXfmBusinessRules.CCL_APP_CCL_FORM_CAT_ID) ELSE ""))) = 0) Then DEFAULT_NULL_VALUE Else InXfmBusinessRules.APPT_FORM_C,
		IFF(
	    {{ ref('ModNullHandling') }}.CCC_APP_CC_APP_CAT_ID IS NOT NULL AND {{ ref('ModNullHandling') }}.CCC_APP_CC_APP_CAT_ID = '1' AND ApptQlfyC = 'CC', 'PSSF',     
	    IFF(
	        {{ ref('ModNullHandling') }}.CCC_APP_CC_APP_CAT_ID IS NOT NULL
	    and {{ ref('ModNullHandling') }}.CCC_APP_CC_APP_CAT_ID = '2'
	    and ApptQlfyC = 'CC', 'PASF',         
	        IFF(
	            {{ ref('ModNullHandling') }}.CCC_APP_CC_APP_CAT_ID IS NOT NULL
	        and {{ ref('ModNullHandling') }}.CCC_APP_CC_APP_CAT_ID = '3'
	        and ApptQlfyC = 'CC', 'FAFF', 
	            IFF(ApptQlfyC <> 'CL'
	            or LEN(TRIM(IFF({{ ref('ModNullHandling') }}.CCL_APP_CCL_FORM_CAT_ID IS NOT NULL, {{ ref('ModNullHandling') }}.CCL_APP_CCL_FORM_CAT_ID, ''))) = 0, DEFAULT_NULL_VALUE, {{ ref('ModNullHandling') }}.APPT_FORM_C)
	        )
	    )
	) AS ApptFormC,
		-- *SRC*: \(20)If Len(Trim(( IF IsNotNull((InXfmBusinessRules.APP_CHANNEL_CAT_ID)) THEN (InXfmBusinessRules.APP_CHANNEL_CAT_ID) ELSE ""))) = 0 Then DEFAULT_NULL_VALUE Else InXfmBusinessRules.APPT_ORIG_C,
		IFF(LEN(TRIM(IFF({{ ref('ModNullHandling') }}.APP_CHANNEL_CAT_ID IS NOT NULL, {{ ref('ModNullHandling') }}.APP_CHANNEL_CAT_ID, ''))) = 0, DEFAULT_NULL_VALUE, {{ ref('ModNullHandling') }}.APPT_ORIG_C) AS ApptOrigC,
		-- *SRC*: \(20)If (InXfmBusinessRules.APP_FOUND_FLAG = 'N') Or Len(Trim(( IF IsNotNull((InXfmBusinessRules.APP_CREATED_DATE)) THEN (InXfmBusinessRules.APP_CREATED_DATE) ELSE ""))) = 0 Then 'Y' Else 'N',
		IFF({{ ref('ModNullHandling') }}.APP_FOUND_FLAG = 'N' OR LEN(TRIM(IFF({{ ref('ModNullHandling') }}.APP_CREATED_DATE IS NOT NULL, {{ ref('ModNullHandling') }}.APP_CREATED_DATE, ''))) = 0, 'Y', 'N') AS CreatedDtIsNull,
		-- *SRC*: \(20)If (CreatedDtIsNull = 'N') Then ( If IsValid('date', StringToDate(InXfmBusinessRules.APP_CREATED_DATE, '%yyyy%mm%dd')) Then 'N' Else 'Y') Else 'N',
		IFF(CreatedDtIsNull = 'N', IFF(ISVALID('date', STRINGTODATE({{ ref('ModNullHandling') }}.APP_CREATED_DATE, '%yyyy%mm%dd')), 'N', 'Y'), 'N') AS ErrorCreatedDt,
		-- *SRC*: \(20)If Len(Trim(( IF IsNotNull((InXfmBusinessRules.APP_SM_CASE_ID)) THEN (InXfmBusinessRules.APP_SM_CASE_ID) ELSE ""))) = 0 then 'N' Else 'Y',
		IFF(LEN(TRIM(IFF({{ ref('ModNullHandling') }}.APP_SM_CASE_ID IS NOT NULL, {{ ref('ModNullHandling') }}.APP_SM_CASE_ID, ''))) = 0, 'N', 'Y') AS LoadSMCase,
		-- *SRC*: \(20)If Len(Trim(( IF IsNotNull((InXfmBusinessRules.APP_CREATED_BY_STAFF_NUMBER)) THEN (InXfmBusinessRules.APP_CREATED_BY_STAFF_NUMBER) ELSE ""))) = 0 then 'N' Else 'Y',
		IFF(LEN(TRIM(IFF({{ ref('ModNullHandling') }}.APP_CREATED_BY_STAFF_NUMBER IS NOT NULL, {{ ref('ModNullHandling') }}.APP_CREATED_BY_STAFF_NUMBER, ''))) = 0, 'N', 'Y') AS LoadApptEmpl1,
		-- *SRC*: \(20)If Len(Trim(( IF IsNotNull((InXfmBusinessRules.APP_OWNED_BY_STAFF_NUMBER)) THEN (InXfmBusinessRules.APP_OWNED_BY_STAFF_NUMBER) ELSE ""))) = 0 then 'N' Else 'Y',
		IFF(LEN(TRIM(IFF({{ ref('ModNullHandling') }}.APP_OWNED_BY_STAFF_NUMBER IS NOT NULL, {{ ref('ModNullHandling') }}.APP_OWNED_BY_STAFF_NUMBER, ''))) = 0, 'N', 'Y') AS LoadApptEmpl2,
		-- *SRC*: \(20)If Len(Trim(( IF IsNotNull((InXfmBusinessRules.APP_LODGEMENT_BRANCH_ID)) THEN (InXfmBusinessRules.APP_LODGEMENT_BRANCH_ID) ELSE ""))) = 0 then 'N' Else 'Y',
		IFF(LEN(TRIM(IFF({{ ref('ModNullHandling') }}.APP_LODGEMENT_BRANCH_ID IS NOT NULL, {{ ref('ModNullHandling') }}.APP_LODGEMENT_BRANCH_ID, ''))) = 0, 'N', 'Y') AS LoadApptDept,
		-- *SRC*: \(20)If LoadApptQlfyC = @FALSE Then 'RPR5101' Else '',
		IFF(LoadApptQlfyC = @FALSE, 'RPR5101', '') AS ErrorCode_1,
		-- *SRC*: \(20)If (InXfmBusinessRules.CCL_APP_FOUND_FLAG = 'Y' and ApptC = '9999') or (trim(InXfmBusinessRules.APP_SUBTYPE_CODE) <> '-1' and TuApptC = '9999') then 'RPR5102' Else '',
		IFF({{ ref('ModNullHandling') }}.CCL_APP_FOUND_FLAG = 'Y' AND ApptC = '9999' OR TRIM({{ ref('ModNullHandling') }}.APP_SUBTYPE_CODE) <> '-1' AND TuApptC = '9999', 'RPR5102', '') AS ErrorCode_2,
		-- *SRC*: \(20)If ApptFormC = '9999' then 'RPR5103' Else '',
		IFF(ApptFormC = '9999', 'RPR5103', '') AS ErrorCode_3,
		-- *SRC*: \(20)If ApptOrigC = '9999' then 'RPR5104' Else '',
		IFF(ApptOrigC = '9999', 'RPR5104', '') AS ErrorCode_4,
		-- *SRC*: \(20)If len(trim(( IF IsNotNull((InXfmBusinessRules.REL_MANAGER_STATE_ID)) THEN (InXfmBusinessRules.REL_MANAGER_STATE_ID) ELSE ""))) <> 0 and InXfmBusinessRules.STAT_X = 'UNK' then 'RPR5107' else '',
		IFF(LEN(TRIM(IFF({{ ref('ModNullHandling') }}.REL_MANAGER_STATE_ID IS NOT NULL, {{ ref('ModNullHandling') }}.REL_MANAGER_STATE_ID, ''))) <> 0 AND {{ ref('ModNullHandling') }}.STAT_X = 'UNK', 'RPR5107', '') AS sErrorC5,
		-- *SRC*: \(20)If len(trim(( IF IsNotNull((InXfmBusinessRules.ORIG_SRCE_SYST_I)) THEN (InXfmBusinessRules.ORIG_SRCE_SYST_I) ELSE ""))) <> 0 and InXfmBusinessRules.STAT_X = 'UNK' then 'RPR5108' else '',
		IFF(LEN(TRIM(IFF({{ ref('ModNullHandling') }}.ORIG_SRCE_SYST_I IS NOT NULL, {{ ref('ModNullHandling') }}.ORIG_SRCE_SYST_I, ''))) <> 0 AND {{ ref('ModNullHandling') }}.STAT_X = 'UNK', 'RPR5108', '') AS sErrorC6,
		-- *SRC*: \(20)If (( IF IsNotNull((InXfmBusinessRules.CHL_TPB_FOUND_FLAG)) THEN (InXfmBusinessRules.CHL_TPB_FOUND_FLAG) ELSE "") = 'N') Or Len(Trim(( IF IsNotNull((InXfmBusinessRules.DATE_RECEIVED)) THEN (InXfmBusinessRules.DATE_RECEIVED) ELSE ""))) = 0 Then 'Y' Else 'N',
		IFF(IFF({{ ref('ModNullHandling') }}.CHL_TPB_FOUND_FLAG IS NOT NULL, {{ ref('ModNullHandling') }}.CHL_TPB_FOUND_FLAG, '') = 'N' OR LEN(TRIM(IFF({{ ref('ModNullHandling') }}.DATE_RECEIVED IS NOT NULL, {{ ref('ModNullHandling') }}.DATE_RECEIVED, ''))) = 0, 'Y', 'N') AS ReceiveDtIsNull,
		-- *SRC*: \(20)If (ReceiveDtIsNull = 'N') Then ( if IsValid('timestamp', StringToTimestamp(InXfmBusinessRules.DATE_RECEIVED, '%yyyy%mm%dd%hh%nn%ss')) Then 'N' Else 'Y') Else 'N',
		IFF(ReceiveDtIsNull = 'N', IFF(ISVALID('timestamp', STRINGTOTIMESTAMP({{ ref('ModNullHandling') }}.DATE_RECEIVED, '%yyyy%mm%dd%hh%nn%ss')), 'N', 'Y'), 'N') AS sErrorC7,
		-- *SRC*: \(20)If ErrorCode_1 <> "" Then ErrorCode_1 Else  If ErrorCode_2 <> "" Then ErrorCode_2 Else  If ErrorCode_3 <> "" Then ErrorCode_3 Else  If ErrorCode_4 <> "" Then ErrorCode_4 Else  If sErrorC5 <> "" Then sErrorC5 Else  IF sErrorCode <> "" Then sErrorCode Else  IF sErrCode <> "" Then sErrCode Else '',
		IFF(ErrorCode_1 <> '', ErrorCode_1, IFF(ErrorCode_2 <> '', ErrorCode_2, IFF(ErrorCode_3 <> '', ErrorCode_3, IFF(ErrorCode_4 <> '', ErrorCode_4, IFF(sErrorC5 <> '', sErrorC5, IFF(sErrorCode <> '', sErrorCode, IFF(sErrCode <> '', sErrCode, ''))))))) AS ErrorCode,
		-- *SRC*: \(20)If ErrorCode <> "" Then @TRUE Else @FALSE,
		IFF(ErrorCode <> '', @TRUE, @FALSE) AS RejectFlag,
		-- *SRC*: \(20)if ApptC = DEFAULT_NULL_VALUE and TuApptC = DEFAULT_NULL_VALUE then 'SETNULL()' else  if ApptC = DEFAULT_NULL_VALUE then TuApptC else ApptC,
		IFF(ApptC = DEFAULT_NULL_VALUE AND TuApptC = DEFAULT_NULL_VALUE, 'SETNULL()', IFF(ApptC = DEFAULT_NULL_VALUE, TuApptC, ApptC)) AS svApptCHLM,
		-- *SRC*: \(20)if ApptQlfyC = 'HM' and IsNull(InXfmBusinessRules.HLM_APP_TYPE_CAT_ID) then DEFAULT_NULL_VALUE else InXfmBusinessRules.APPT_C_HLM,
		IFF(ApptQlfyC = 'HM' AND {{ ref('ModNullHandling') }}.HLM_APP_TYPE_CAT_ID IS NULL, DEFAULT_NULL_VALUE, {{ ref('ModNullHandling') }}.APPT_C_HLM) AS svCHLM,
		-- *SRC*: \(20)IF ((TRIM(InXfmBusinessRules.APPT_QLFY_C)) = '99' OR svCHLM = '9999') THEN "Y" ELSE "N",
		IFF(TRIM({{ ref('ModNullHandling') }}.APPT_QLFY_C) = '99' OR svCHLM = '9999', 'Y', 'N') AS sErrorCode,
		-- *SRC*: \(20)IF IsNull(InXfmBusinessRules.ORIG_APPT_SRCE_SYST_C) THEN '9999' ELSE InXfmBusinessRules.ORIG_APPT_SRCE_SYST_C,
		IFF({{ ref('ModNullHandling') }}.ORIG_APPT_SRCE_SYST_C IS NULL, '9999', {{ ref('ModNullHandling') }}.ORIG_APPT_SRCE_SYST_C) AS IsNullOrigApptSrceSystc,
		-- *SRC*: \(20)IF IsNull(InXfmBusinessRules.HL_BUSN_CHNL_CAT_I) Then "N/A" Else  If Trim(InXfmBusinessRules.HL_BUSN_CHNL_CAT_I) = '' Then "N/A" Else InXfmBusinessRules.HL_BUSN_CHNL_CAT_I,
		IFF({{ ref('ModNullHandling') }}.HL_BUSN_CHNL_CAT_I IS NULL, 'N/A', IFF(TRIM({{ ref('ModNullHandling') }}.HL_BUSN_CHNL_CAT_I) = '', 'N/A', {{ ref('ModNullHandling') }}.HL_BUSN_CHNL_CAT_I)) AS svApptc,
		-- *SRC*: \(20)IF (InXfmBusinessRules.APPT_QLFY_C = "HM" AND svApptc = "N/A") Then "N/A" ELSE  IF InXfmBusinessRules.APPT_QLFY_C = 'HM' THEN IsNullOrigApptSrceSystc ELSE  If len(trim(( IF IsNotNull((InXfmBusinessRules.ORIG_SRCE_SYST_I)) THEN (InXfmBusinessRules.ORIG_SRCE_SYST_I) ELSE ""))) <> 0 then InXfmBusinessRules.ORIG_SRCE_SYST_C Else 'E01',
		IFF({{ ref('ModNullHandling') }}.APPT_QLFY_C = 'HM' AND svApptc = 'N/A', 'N/A', IFF({{ ref('ModNullHandling') }}.APPT_QLFY_C = 'HM', IsNullOrigApptSrceSystc, IFF(LEN(TRIM(IFF({{ ref('ModNullHandling') }}.ORIG_SRCE_SYST_I IS NOT NULL, {{ ref('ModNullHandling') }}.ORIG_SRCE_SYST_I, ''))) <> 0, {{ ref('ModNullHandling') }}.ORIG_SRCE_SYST_C, 'E01'))) AS svOrigApptSrceSystc,
		-- *SRC*: \(20)If svOrigApptSrceSystc = '9999' Then 'Y' Else 'N',
		IFF(svOrigApptSrceSystc = '9999', 'Y', 'N') AS sErrCode,
		-- *SRC*: \(20)If IsNull(InXfmBusinessRules.DATE_RECEIVED) Or InXfmBusinessRules.DATE_RECEIVED = '0' Or Trim(( IF IsNotNull((InXfmBusinessRules.DATE_RECEIVED)) THEN (InXfmBusinessRules.DATE_RECEIVED) ELSE "")) = '' then 'N' Else 'Y',
		IFF({{ ref('ModNullHandling') }}.DATE_RECEIVED IS NULL OR {{ ref('ModNullHandling') }}.DATE_RECEIVED = '0' OR TRIM(IFF({{ ref('ModNullHandling') }}.DATE_RECEIVED IS NOT NULL, {{ ref('ModNullHandling') }}.DATE_RECEIVED, '')) = '', 'N', 'Y') AS IsNullDateRecv,
		-- *SRC*: \(20)If IsNullDateRecv = 'Y' And IsValid('date', Trim(( IF IsNotNull((InXfmBusinessRules.DATE_RECEIVED)) THEN (InXfmBusinessRules.DATE_RECEIVED) ELSE "")[1, 4]) : '-' : Trim(( IF IsNotNull((InXfmBusinessRules.DATE_RECEIVED)) THEN (InXfmBusinessRules.DATE_RECEIVED) ELSE "")[5, 2]) : '-' : Trim(( IF IsNotNull((InXfmBusinessRules.DATE_RECEIVED)) THEN (InXfmBusinessRules.DATE_RECEIVED) ELSE "")[7, 2])) Then 'Y' Else 'N',
		IFF(    
	    IsNullDateRecv = 'Y'
	    and ISVALID('date', CONCAT(CONCAT(CONCAT(CONCAT(TRIM(IFF({{ ref('ModNullHandling') }}.DATE_RECEIVED IS NOT NULL, {{ ref('ModNullHandling') }}.DATE_RECEIVED, '')), '-'), TRIM(IFF({{ ref('ModNullHandling') }}.DATE_RECEIVED IS NOT NULL, {{ ref('ModNullHandling') }}.DATE_RECEIVED, ''))), '-'), TRIM(IFF({{ ref('ModNullHandling') }}.DATE_RECEIVED IS NOT NULL, {{ ref('ModNullHandling') }}.DATE_RECEIVED, '')))), 
	    'Y', 
	    'N'
	) AS IsValidDateRecv,
		-- *SRC*: \(20)If IsNull(InXfmBusinessRules.APP_CREATED_DATE) Or InXfmBusinessRules.APP_CREATED_DATE = '0' Or Trim(( IF IsNotNull((InXfmBusinessRules.APP_CREATED_DATE)) THEN (InXfmBusinessRules.APP_CREATED_DATE) ELSE "")) = '' then 'N' Else 'Y',
		IFF({{ ref('ModNullHandling') }}.APP_CREATED_DATE IS NULL OR {{ ref('ModNullHandling') }}.APP_CREATED_DATE = '0' OR TRIM(IFF({{ ref('ModNullHandling') }}.APP_CREATED_DATE IS NOT NULL, {{ ref('ModNullHandling') }}.APP_CREATED_DATE, '')) = '', 'N', 'Y') AS IsNullCrtdDate,
		-- *SRC*: \(20)If IsNullCrtdDate = 'Y' And IsValid('date', Trim(( IF IsNotNull((InXfmBusinessRules.APP_CREATED_DATE)) THEN (InXfmBusinessRules.APP_CREATED_DATE) ELSE "")[1, 4]) : '-' : Trim(( IF IsNotNull((InXfmBusinessRules.APP_CREATED_DATE)) THEN (InXfmBusinessRules.APP_CREATED_DATE) ELSE "")[5, 2]) : '-' : Trim(( IF IsNotNull((InXfmBusinessRules.APP_CREATED_DATE)) THEN (InXfmBusinessRules.APP_CREATED_DATE) ELSE "")[7, 2])) Then 'Y' Else 'N',
		IFF(    
	    IsNullCrtdDate = 'Y'
	    and ISVALID('date', CONCAT(CONCAT(CONCAT(CONCAT(TRIM(IFF({{ ref('ModNullHandling') }}.APP_CREATED_DATE IS NOT NULL, {{ ref('ModNullHandling') }}.APP_CREATED_DATE, '')), '-'), TRIM(IFF({{ ref('ModNullHandling') }}.APP_CREATED_DATE IS NOT NULL, {{ ref('ModNullHandling') }}.APP_CREATED_DATE, ''))), '-'), TRIM(IFF({{ ref('ModNullHandling') }}.APP_CREATED_DATE IS NOT NULL, {{ ref('ModNullHandling') }}.APP_CREATED_DATE, '')))), 
	    'Y', 
	    'N'
	) AS IsValidCrtdDate,
		{{ ref('ModNullHandling') }}.APP_ID AS SRCE_KEY_I,
		'ConversionTimestamp' AS CONV_M,
		'SRCTRMCHECK' AS CONV_MAP_RULE_M,
		'N/A' AS TRSF_TABL_M,
		-- *SRC*: ( IF IsNotNull((InXfmBusinessRules.APP_CREATED_DATE)) THEN (InXfmBusinessRules.APP_CREATED_DATE) ELSE ""),
		IFF({{ ref('ModNullHandling') }}.APP_CREATED_DATE IS NOT NULL, {{ ref('ModNullHandling') }}.APP_CREATED_DATE, '') AS VALU_CHNG_BFOR_X,
		'1111-11-11' AS VALU_CHNG_AFTR_X,
		-- *SRC*: 'Job Name = ' : DSJobName,
		CONCAT('Job Name = ', DSJobName) AS TRSF_X,
		'APPT_CRAT_D' AS TRSF_COLM_M
	FROM {{ ref('ModNullHandling') }}
	WHERE ErrorCreatedDt = 'Y'
)

SELECT * FROM XfmBusinessRules__OutErrorRecvDate