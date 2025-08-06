{{ config(materialized='view', tags=['XfmCSE_CCL_BUS_APP_FEEFrmExt']) }}

WITH XfmBusinessRules__OutTmpApptPdctFeatDS AS (
	SELECT
		{{ ref('ModNullHandling') }}.TARG_CHAR_C AS svFeatI,
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((InXfmBusinessRules.CCL_APP_FEE_OVERRIDE_FEE_PCT_FREQ)) THEN (InXfmBusinessRules.CCL_APP_FEE_OVERRIDE_FEE_PCT_FREQ) ELSE ""))) = 0) or (InXfmBusinessRules.CCL_APP_FEE_OVERRIDE_FEE_PCT_FREQ = 0) Or InXfmBusinessRules.FREQ_IN_MTHS = 9999 Then DEFAULT_NULL_VALUE ELSE DecimalToString(InXfmBusinessRules.FREQ_IN_MTHS),
		IFF(
	    LEN(TRIM(IFF({{ ref('ModNullHandling') }}.CCL_APP_FEE_OVERRIDE_FEE_PCT_FREQ IS NOT NULL, {{ ref('ModNullHandling') }}.CCL_APP_FEE_OVERRIDE_FEE_PCT_FREQ, ''))) = 0 OR {{ ref('ModNullHandling') }}.CCL_APP_FEE_OVERRIDE_FEE_PCT_FREQ = 0 OR {{ ref('ModNullHandling') }}.FREQ_IN_MTHS = 9999, DEFAULT_NULL_VALUE, 
	    DECIMALTOSTRING({{ ref('ModNullHandling') }}.FREQ_IN_MTHS)
	) AS svActlValuQ,
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((InXfmBusinessRules.CCL_APP_FEE_CCL_APP_ID)) THEN (InXfmBusinessRules.CCL_APP_FEE_CCL_APP_ID) ELSE ""))) <> 0 and Len(Trim(( IF IsNotNull((InXfmBusinessRules.CCL_APP_FEE_CCL_APP_PROD_ID)) THEN (InXfmBusinessRules.CCL_APP_FEE_CCL_APP_PROD_ID) ELSE ""))) = 0) Then 'Y' Else 'N',
		IFF(LEN(TRIM(IFF({{ ref('ModNullHandling') }}.CCL_APP_FEE_CCL_APP_ID IS NOT NULL, {{ ref('ModNullHandling') }}.CCL_APP_FEE_CCL_APP_ID, ''))) <> 0 AND LEN(TRIM(IFF({{ ref('ModNullHandling') }}.CCL_APP_FEE_CCL_APP_PROD_ID IS NOT NULL, {{ ref('ModNullHandling') }}.CCL_APP_FEE_CCL_APP_PROD_ID, ''))) = 0, 'Y', 'N') AS svLoadApptFeatCond1,
		-- *SRC*: \(20)if ((Len(Trim(( IF IsNotNull((InXfmBusinessRules.CCL_APP_FEE_OVERRIDE_FEE_PCT)) THEN (InXfmBusinessRules.CCL_APP_FEE_OVERRIDE_FEE_PCT) ELSE ""))) = 0 or InXfmBusinessRules.CCL_APP_FEE_OVERRIDE_FEE_PCT = 0) and (Len(Trim(( IF IsNotNull((InXfmBusinessRules.CCL_APP_FEE_CHARGE_AMT)) THEN (InXfmBusinessRules.CCL_APP_FEE_CHARGE_AMT) ELSE ""))) = 0 or InXfmBusinessRules.CCL_APP_FEE_CHARGE_AMT = 0)) Then 'N' Else 'Y',
		IFF(    
	    LEN(TRIM(IFF({{ ref('ModNullHandling') }}.CCL_APP_FEE_OVERRIDE_FEE_PCT IS NOT NULL, {{ ref('ModNullHandling') }}.CCL_APP_FEE_OVERRIDE_FEE_PCT, ''))) = 0
	    or {{ ref('ModNullHandling') }}.CCL_APP_FEE_OVERRIDE_FEE_PCT = 0
	    and LEN(TRIM(IFF({{ ref('ModNullHandling') }}.CCL_APP_FEE_CHARGE_AMT IS NOT NULL, {{ ref('ModNullHandling') }}.CCL_APP_FEE_CHARGE_AMT, ''))) = 0
	    or {{ ref('ModNullHandling') }}.CCL_APP_FEE_CHARGE_AMT = 0, 
	    'N', 
	    'Y'
	) AS svFeePctOrChargeAmountValid,
		-- *SRC*: \(20)If svFeatI = '9999' then 'RPR2101' else  if svFeatI = DEFAULT_NULL_VALUE then 'REJ2002' else '',
		IFF(svFeatI = '9999', 'RPR2101', IFF(svFeatI = DEFAULT_NULL_VALUE, 'REJ2002', '')) AS svErrorCode,
		-- *SRC*: \(20)If svErrorCode <> "" Then @TRUE Else @FALSE,
		IFF(svErrorCode <> '', @TRUE, @FALSE) AS svRejectFlag,
		-- *SRC*: \(20)If svFeatI = DEFAULT_NULL_VALUE then 'N' else 'Y',
		IFF(svFeatI = DEFAULT_NULL_VALUE, 'N', 'Y') AS svLoadFeatI,
		-- *SRC*: \(20)if Len(Trim(( IF IsNotNull((InXfmBusinessRules.CCL_APP_FEE_CHARGE_DATE)) THEN (InXfmBusinessRules.CCL_APP_FEE_CHARGE_DATE) ELSE ""))) <> 0 Then  If IsValid('date', StringToDate(InXfmBusinessRules.CCL_APP_FEE_CHARGE_DATE, '%yyyy%mm%dd')) Then 'N' Else 'Y' Else 'N',
		IFF(LEN(TRIM(IFF({{ ref('ModNullHandling') }}.CCL_APP_FEE_CHARGE_DATE IS NOT NULL, {{ ref('ModNullHandling') }}.CCL_APP_FEE_CHARGE_DATE, ''))) <> 0, IFF(ISVALID('date', STRINGTODATE({{ ref('ModNullHandling') }}.CCL_APP_FEE_CHARGE_DATE, '%yyyy%mm%dd')), 'N', 'Y'), 'N') AS svErrorDate,
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((InXfmBusinessRules.CCL_APP_FEE_CCL_APP_PROD_ID)) THEN (InXfmBusinessRules.CCL_APP_FEE_CCL_APP_PROD_ID) ELSE ""))) = 0) Then 'N' Else 'Y',
		IFF(LEN(TRIM(IFF({{ ref('ModNullHandling') }}.CCL_APP_FEE_CCL_APP_PROD_ID IS NOT NULL, {{ ref('ModNullHandling') }}.CCL_APP_FEE_CCL_APP_PROD_ID, ''))) = 0, 'N', 'Y') AS svLoadApptPdctFeatCond1,
		-- *SRC*: \(20)If svFeatI = '999010' then 'RPR3108' else '',
		IFF(svFeatI = '999010', 'RPR3108', '') AS svErrorCode1,
		-- *SRC*: \(20)If svActlValuQ = '9999' then 'RPR3109' else '',
		IFF(svActlValuQ = '9999', 'RPR3109', '') AS svErrorCode2,
		-- *SRC*: 'CSECL' : InXfmBusinessRules.CCL_APP_FEE_CCL_APP_PROD_ID,
		CONCAT('CSECL', {{ ref('ModNullHandling') }}.CCL_APP_FEE_CCL_APP_PROD_ID) AS APPT_PDCT_I,
		svFeatI AS FEAT_I,
		-- *SRC*: 'CF' : InXfmBusinessRules.CCL_APP_FEE_ID,
		CONCAT('CF', {{ ref('ModNullHandling') }}.CCL_APP_FEE_ID) AS SRCE_SYST_APPT_FEAT_I,
		-- *SRC*: StringToDate(ETL_PROCESS_DT, "%yyyy%mm%dd"),
		STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd') AS EFFT_D,
		'CSE' AS SRCE_SYST_C,
		-- *SRC*: SetNull(),
		SETNULL() AS SRCE_SYST_APPT_OVRD_I,
		-- *SRC*: SetNull(),
		SETNULL() AS OVRD_FEAT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS SRCE_SYST_STND_VALU_Q,
		-- *SRC*: \(20)If Len(Trim(( IF IsNotNull((InXfmBusinessRules.CCL_FEE_TYPE_CAT_DEFAULT_FEE_PCT)) THEN (InXfmBusinessRules.CCL_FEE_TYPE_CAT_DEFAULT_FEE_PCT) ELSE ""))) = 0 Then SetNull() Else StringToDecimal(InXfmBusinessRules.CCL_FEE_TYPE_CAT_DEFAULT_FEE_PCT),
		IFF(LEN(TRIM(IFF({{ ref('ModNullHandling') }}.CCL_FEE_TYPE_CAT_DEFAULT_FEE_PCT IS NOT NULL, {{ ref('ModNullHandling') }}.CCL_FEE_TYPE_CAT_DEFAULT_FEE_PCT, ''))) = 0, SETNULL(), STRINGTODECIMAL({{ ref('ModNullHandling') }}.CCL_FEE_TYPE_CAT_DEFAULT_FEE_PCT)) AS SRCE_SYST_STND_VALU_R,
		-- *SRC*: \(20)If Len(Trim(( IF IsNotNull((InXfmBusinessRules.CCL_FEE_TYPE_CAT_DEFAULT_AMT)) THEN (InXfmBusinessRules.CCL_FEE_TYPE_CAT_DEFAULT_AMT) ELSE ""))) = 0 Then SetNull() Else StringToDecimal(InXfmBusinessRules.CCL_FEE_TYPE_CAT_DEFAULT_AMT),
		IFF(LEN(TRIM(IFF({{ ref('ModNullHandling') }}.CCL_FEE_TYPE_CAT_DEFAULT_AMT IS NOT NULL, {{ ref('ModNullHandling') }}.CCL_FEE_TYPE_CAT_DEFAULT_AMT, ''))) = 0, SETNULL(), STRINGTODECIMAL({{ ref('ModNullHandling') }}.CCL_FEE_TYPE_CAT_DEFAULT_AMT)) AS SRCE_SYST_STND_VALU_A,
		'AUD' AS CNCY_C,
		-- *SRC*: \(20)if svActlValuQ = DEFAULT_NULL_VALUE then SetNull() else svActlValuQ,
		IFF(svActlValuQ = DEFAULT_NULL_VALUE, SETNULL(), svActlValuQ) AS ACTL_VALU_Q,
		-- *SRC*: StringToDecimal(InXfmBusinessRules.CCL_APP_FEE_OVERRIDE_FEE_PCT),
		STRINGTODECIMAL({{ ref('ModNullHandling') }}.CCL_APP_FEE_OVERRIDE_FEE_PCT) AS ACTL_VALU_R,
		-- *SRC*: StringToDecimal(InXfmBusinessRules.CCL_APP_FEE_CHARGE_AMT),
		STRINGTODECIMAL({{ ref('ModNullHandling') }}.CCL_APP_FEE_CHARGE_AMT) AS ACTL_VALU_A,
		-- *SRC*: SetNull(),
		SETNULL() AS FEAT_SEQN_N,
		-- *SRC*: SetNull(),
		SETNULL() AS FEAT_STRT_D,
		-- *SRC*: \(20)if Len(Trim(( IF IsNotNull((InXfmBusinessRules.CCL_APP_FEE_CHARGE_DATE)) THEN (InXfmBusinessRules.CCL_APP_FEE_CHARGE_DATE) ELSE ""))) = 0 then SetNull() else  if svErrorDate = 'Y' then StringToDate(DEFAULT_DT, '%yyyy%mm%dd') else StringToDate(InXfmBusinessRules.CCL_APP_FEE_CHARGE_DATE, "%yyyy%mm%dd"),
		IFF(LEN(TRIM(IFF({{ ref('ModNullHandling') }}.CCL_APP_FEE_CHARGE_DATE IS NOT NULL, {{ ref('ModNullHandling') }}.CCL_APP_FEE_CHARGE_DATE, ''))) = 0, SETNULL(), IFF(svErrorDate = 'Y', STRINGTODATE(DEFAULT_DT, '%yyyy%mm%dd'), STRINGTODATE({{ ref('ModNullHandling') }}.CCL_APP_FEE_CHARGE_DATE, '%yyyy%mm%dd'))) AS FEE_CHRG_D,
		-- *SRC*: SetNull(),
		SETNULL() AS OVRD_REAS_C,
		'N' AS FEE_ADD_TO_TOTL_F,
		'N' AS FEE_CAPL_F,
		-- *SRC*: StringToDate('99991231', '%yyyy%mm%dd'),
		STRINGTODATE('99991231', '%yyyy%mm%dd') AS EXPY_D,
		0 AS PROS_KEY_EFFT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS PROS_KEY_EXPY_I,
		-- *SRC*: SetNull(),
		SETNULL() AS EROR_SEQN_I,
		RUN_STREAM AS RUN_STRM,
		'3' AS DLTA_VERS,
		'DUMY' AS FEAT_TYPE_C
	FROM {{ ref('ModNullHandling') }}
	WHERE svLoadApptPdctFeatCond1 = 'Y' AND svFeePctOrChargeAmountValid = 'Y'
)

SELECT * FROM XfmBusinessRules__OutTmpApptPdctFeatDS