{{ config(materialized='view', tags=['XfmCSE_CCL_BUS_APP_FEEFrmExt']) }}

WITH XfmBusinessRules__OutCCAppProdRejectsDS AS (
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
		CCL_APP_FEE_ID,
		CCL_APP_FEE_CCL_APP_ID,
		CCL_APP_FEE_CCL_APP_PROD_ID,
		CCL_APP_FEE_CHARGE_AMT,
		CCL_APP_FEE_CHARGE_DATE,
		CCL_APP_FEE_CONCESSION_FLAG,
		CCL_APP_FEE_CONCESSION_REASON,
		CCL_APP_FEE_OVERRIDE_FEE_PCT,
		CCL_APP_FEE_OVERRIDE_FEE_PCT_FREQ,
		CCL_APP_FEE_FEE_REPAYMENT_FREQ,
		CCL_APP_FEE_TYPE_CAT_ID,
		CCL_APP_FEE_CHARGE_EXTERNAL_FLAG,
		-- *SRC*: DecimalToString(InXfmBusinessRules.CCL_FEE_TYPE_CAT_ID),
		DECIMALTOSTRING({{ ref('ModNullHandling') }}.CCL_FEE_TYPE_CAT_ID) AS CCL_FEE_TYPE_CAT_ID,
		CCL_FEE_TYPE_CAT_DEFAULT_AMT,
		CCL_FEE_TYPE_CAT_DEFAULT_FEE_PCT,
		CCL_FEE_TYPE_CAT_FEE_TYPE_DESC,
		ETL_PROCESS_DT AS ETL_D,
		ORIG_ETL_D,
		svErrorCode AS EROR_C
	FROM {{ ref('ModNullHandling') }}
	WHERE svRejectFlag
)

SELECT * FROM XfmBusinessRules__OutCCAppProdRejectsDS