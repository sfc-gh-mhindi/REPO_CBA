{{ config(materialized='view', tags=['XfmPL_APP_PRODFrmExt']) }}

WITH XfmBusinessRules__OutErrorTargChar AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((InXfmBusinessRules.TP_BROKER_GROUP_CAT_ID)) THEN (InXfmBusinessRules.TP_BROKER_GROUP_CAT_ID) ELSE ""))) = 0) Then DEFAULT_NULL_VALUE ELSE InXfmBusinessRules.UNID_PATY_CATG_C,
		IFF(LEN(TRIM(IFF({{ ref('ModNullHandling') }}.TP_BROKER_GROUP_CAT_ID IS NOT NULL, {{ ref('ModNullHandling') }}.TP_BROKER_GROUP_CAT_ID, ''))) = 0, DEFAULT_NULL_VALUE, {{ ref('ModNullHandling') }}.UNID_PATY_CATG_C) AS svUnidPatyCatg,
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((InXfmBusinessRules.PL_PROD_TERM_CAT_ID)) THEN (InXfmBusinessRules.PL_PROD_TERM_CAT_ID) ELSE ""))) = 0) Then DEFAULT_NULL_VALUE ELSE DecimalToString(InXfmBusinessRules.SRCE_SYST_ACTL_TERM_Q),
		IFF(LEN(TRIM(IFF({{ ref('ModNullHandling') }}.PL_PROD_TERM_CAT_ID IS NOT NULL, {{ ref('ModNullHandling') }}.PL_PROD_TERM_CAT_ID, ''))) = 0, DEFAULT_NULL_VALUE, DECIMALTOSTRING({{ ref('ModNullHandling') }}.SRCE_SYST_ACTL_TERM_Q)) AS svSrcSystActlTerm,
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((InXfmBusinessRules.REPAY_FREQUENCY_ID)) THEN (InXfmBusinessRules.REPAY_FREQUENCY_ID) ELSE ""))) = 0) Then DEFAULT_NULL_VALUE ELSE InXfmBusinessRules.PAYT_FREQ_C,
		IFF(LEN(TRIM(IFF({{ ref('ModNullHandling') }}.REPAY_FREQUENCY_ID IS NOT NULL, {{ ref('ModNullHandling') }}.REPAY_FREQUENCY_ID, ''))) = 0, DEFAULT_NULL_VALUE, {{ ref('ModNullHandling') }}.PAYT_FREQ_C) AS svPaytFreqC,
		-- *SRC*: \(20)If svUnidPatyCatg = '9999' then 'RPR1104' else '',
		IFF(svUnidPatyCatg = '9999', 'RPR1104', '') AS svErrorCode1,
		-- *SRC*: \(20)if InXfmBusinessRules.TARG_CHAR_C = '999007' then 'RPR1105' else '',
		IFF({{ ref('ModNullHandling') }}.TARG_CHAR_C = '999007', 'RPR1105', '') AS svErrorCode2,
		-- *SRC*: \(20)if svSrcSystActlTerm = 9999 then 'RPR1106' else '',
		IFF(svSrcSystActlTerm = 9999, 'RPR1106', '') AS svErrorCode3,
		-- *SRC*: \(20)if svPaytFreqC = '9' then 'RPR1107' else '',
		IFF(svPaytFreqC = '9', 'RPR1107', '') AS svErrorCode5,
		-- *SRC*: \(20)if InXfmBusinessRules.TARG_CHAR_C2 = '999012' then 'RPR1109' else '',
		IFF({{ ref('ModNullHandling') }}.TARG_CHAR_C2 = '999012', 'RPR1109', '') AS svErrorCode6,
		-- *SRC*: \(20)if svErrorCode1 <> '' then svErrorCode1 else  if svErrorCode2 <> '' then svErrorCode2 else  if svErrorCode3 <> '' then svErrorCode3 else  if svErrorCode5 <> '' then svErrorCode5 else  if svErrorCode6 <> '' then svErrorCode6 else '',
		IFF(svErrorCode1 <> '', svErrorCode1, IFF(svErrorCode2 <> '', svErrorCode2, IFF(svErrorCode3 <> '', svErrorCode3, IFF(svErrorCode5 <> '', svErrorCode5, IFF(svErrorCode6 <> '', svErrorCode6, ''))))) AS svErrorCode,
		-- *SRC*: \(20)If svErrorCode1 <> "" or svErrorCode2 <> "" or svErrorCode3 <> "" or svErrorCode5 <> "" or svErrorCode6 <> "" Then @TRUE Else @FALSE,
		IFF(svErrorCode1 <> '' OR svErrorCode2 <> '' OR svErrorCode3 <> '' OR svErrorCode5 <> '' OR svErrorCode6 <> '', @TRUE, @FALSE) AS svRejectFlag,
		-- *SRC*: \(20)if (Len(Trim(( IF IsNotNull((InXfmBusinessRules.HLS_ACCT_NO)) THEN (InXfmBusinessRules.HLS_ACCT_NO) ELSE ""))) = 0) then 'N' else 'Y',
		IFF(LEN(TRIM(IFF({{ ref('ModNullHandling') }}.HLS_ACCT_NO IS NOT NULL, {{ ref('ModNullHandling') }}.HLS_ACCT_NO, ''))) = 0, 'N', 'Y') AS svLoadApptPdctAcct,
		-- *SRC*: \(20)if (Len(Trim(( IF IsNotNull((InXfmBusinessRules.TP_BROKER_ID)) THEN (InXfmBusinessRules.TP_BROKER_ID) ELSE ""))) = 0) then 'N' else 'Y',
		IFF(LEN(TRIM(IFF({{ ref('ModNullHandling') }}.TP_BROKER_ID IS NOT NULL, {{ ref('ModNullHandling') }}.TP_BROKER_ID, ''))) = 0, 'N', 'Y') AS svLoadApptUnidPaty,
		-- *SRC*: \(20)if (Len(Trim(( IF IsNotNull((InXfmBusinessRules.PL_PROD_TERM_CAT_ID)) THEN (InXfmBusinessRules.PL_PROD_TERM_CAT_ID) ELSE ""))) = 0) then 'N' else 'Y',
		IFF(LEN(TRIM(IFF({{ ref('ModNullHandling') }}.PL_PROD_TERM_CAT_ID IS NOT NULL, {{ ref('ModNullHandling') }}.PL_PROD_TERM_CAT_ID, ''))) = 0, 'N', 'Y') AS svLoadApptPdctFeat,
		-- *SRC*: \(20)if (Len(Trim(( IF IsNotNull((InXfmBusinessRules.PL_APP_PROD_REPAY_CAT_ID)) THEN (InXfmBusinessRules.PL_APP_PROD_REPAY_CAT_ID) ELSE ""))) = 0) or (Len(Trim(( IF IsNotNull((InXfmBusinessRules.REPAY_FREQUENCY_ID)) THEN (InXfmBusinessRules.REPAY_FREQUENCY_ID) ELSE ""))) = 0) then 'N' else 'Y',
		IFF(LEN(TRIM(IFF({{ ref('ModNullHandling') }}.PL_APP_PROD_REPAY_CAT_ID IS NOT NULL, {{ ref('ModNullHandling') }}.PL_APP_PROD_REPAY_CAT_ID, ''))) = 0 OR LEN(TRIM(IFF({{ ref('ModNullHandling') }}.REPAY_FREQUENCY_ID IS NOT NULL, {{ ref('ModNullHandling') }}.REPAY_FREQUENCY_ID, ''))) = 0, 'N', 'Y') AS svLoadRPay,
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((InXfmBusinessRules.CAR_SEEKER_FLAG)) THEN (InXfmBusinessRules.CAR_SEEKER_FLAG) ELSE ""))) = 0) Then 'N' else InXfmBusinessRules.CAR_SEEKER_FLAG,
		IFF(LEN(TRIM(IFF({{ ref('ModNullHandling') }}.CAR_SEEKER_FLAG IS NOT NULL, {{ ref('ModNullHandling') }}.CAR_SEEKER_FLAG, ''))) = 0, 'N', {{ ref('ModNullHandling') }}.CAR_SEEKER_FLAG) AS svCarSeekFlag,
		-- *SRC*: \(20)if Len(Trim(( IF IsNotNull((InXfmBusinessRules.PL_APP_PROD_ID)) THEN (InXfmBusinessRules.PL_APP_PROD_ID) ELSE ""))) = 0 Or Trim(InXfmBusinessRules.PL_APP_PROD_ID) = '0' Or Len(Trim(( IF IsNotNull((InXfmBusinessRules.READ_COSTS_AND_RISKS_FLAG)) THEN (InXfmBusinessRules.READ_COSTS_AND_RISKS_FLAG) ELSE ""))) = 0 Or Trim(( IF IsNotNull((InXfmBusinessRules.READ_COSTS_AND_RISKS_FLAG)) THEN (InXfmBusinessRules.READ_COSTS_AND_RISKS_FLAG) ELSE (0))) = '0' then 'N' else 'Y',
		IFF(    
	    LEN(TRIM(IFF({{ ref('ModNullHandling') }}.PL_APP_PROD_ID IS NOT NULL, {{ ref('ModNullHandling') }}.PL_APP_PROD_ID, ''))) = 0
	    or TRIM({{ ref('ModNullHandling') }}.PL_APP_PROD_ID) = '0'
	    or LEN(TRIM(IFF({{ ref('ModNullHandling') }}.READ_COSTS_AND_RISKS_FLAG IS NOT NULL, {{ ref('ModNullHandling') }}.READ_COSTS_AND_RISKS_FLAG, ''))) = 0
	    or TRIM(IFF({{ ref('ModNullHandling') }}.READ_COSTS_AND_RISKS_FLAG IS NOT NULL, {{ ref('ModNullHandling') }}.READ_COSTS_AND_RISKS_FLAG, 0)) = '0', 
	    'N', 
	    'Y'
	) AS svApptPdctChkl1,
		-- *SRC*: \(20)if Len(Trim(( IF IsNotNull((InXfmBusinessRules.PL_APP_PROD_ID)) THEN (InXfmBusinessRules.PL_APP_PROD_ID) ELSE ""))) = 0 Or Trim(InXfmBusinessRules.PL_APP_PROD_ID) = '0' Or Len(Trim(( IF IsNotNull((InXfmBusinessRules.ACCEPTS_COSTS_AND_RISKS_DATE)) THEN (InXfmBusinessRules.ACCEPTS_COSTS_AND_RISKS_DATE) ELSE ""))) = 0 Or Trim(( IF IsNotNull((InXfmBusinessRules.ACCEPTS_COSTS_AND_RISKS_DATE)) THEN (InXfmBusinessRules.ACCEPTS_COSTS_AND_RISKS_DATE) ELSE (0))) = '0' then 'N' else 'Y',
		IFF(    
	    LEN(TRIM(IFF({{ ref('ModNullHandling') }}.PL_APP_PROD_ID IS NOT NULL, {{ ref('ModNullHandling') }}.PL_APP_PROD_ID, ''))) = 0
	    or TRIM({{ ref('ModNullHandling') }}.PL_APP_PROD_ID) = '0'
	    or LEN(TRIM(IFF({{ ref('ModNullHandling') }}.ACCEPTS_COSTS_AND_RISKS_DATE IS NOT NULL, {{ ref('ModNullHandling') }}.ACCEPTS_COSTS_AND_RISKS_DATE, ''))) = 0
	    or TRIM(IFF({{ ref('ModNullHandling') }}.ACCEPTS_COSTS_AND_RISKS_DATE IS NOT NULL, {{ ref('ModNullHandling') }}.ACCEPTS_COSTS_AND_RISKS_DATE, 0)) = '0', 
	    'N', 
	    'Y'
	) AS svApptPdctChkl2,
		-- *SRC*: \(20)if Len(Trim(( IF IsNotNull((InXfmBusinessRules.ACCEPTS_COSTS_AND_RISKS_DATE)) THEN (InXfmBusinessRules.ACCEPTS_COSTS_AND_RISKS_DATE) ELSE ""))) = 0 then '' else InXfmBusinessRules.ACCEPTS_COSTS_AND_RISKS_DATE[1, 4] : '-' : InXfmBusinessRules.ACCEPTS_COSTS_AND_RISKS_DATE[5, 2] : '-' : InXfmBusinessRules.ACCEPTS_COSTS_AND_RISKS_DATE[7, 2],
		IFF(
	    LEN(TRIM(IFF({{ ref('ModNullHandling') }}.ACCEPTS_COSTS_AND_RISKS_DATE IS NOT NULL, {{ ref('ModNullHandling') }}.ACCEPTS_COSTS_AND_RISKS_DATE, ''))) = 0, '', 
	    CONCAT(CONCAT(CONCAT(CONCAT(SUBSTRING({{ ref('ModNullHandling') }}.ACCEPTS_COSTS_AND_RISKS_DATE, 1, 4), '-'), SUBSTRING({{ ref('ModNullHandling') }}.ACCEPTS_COSTS_AND_RISKS_DATE, 5, 2)), '-'), SUBSTRING({{ ref('ModNullHandling') }}.ACCEPTS_COSTS_AND_RISKS_DATE, 7, 2))
	) AS svStusd,
		-- *SRC*: \(20)If IsValid('date', svStusd) Then StringToDate(( IF IsNotNull((InXfmBusinessRules.ACCEPTS_COSTS_AND_RISKS_DATE)) THEN (InXfmBusinessRules.ACCEPTS_COSTS_AND_RISKS_DATE) ELSE ""), '%yyyy%mm%dd') Else StringToDate(DEFAULT_DT, '%yyyy%mm%dd'),
		IFF(ISVALID('date', svStusd), STRINGTODATE(IFF({{ ref('ModNullHandling') }}.ACCEPTS_COSTS_AND_RISKS_DATE IS NOT NULL, {{ ref('ModNullHandling') }}.ACCEPTS_COSTS_AND_RISKS_DATE, ''), '%yyyy%mm%dd'), STRINGTODATE(DEFAULT_DT, '%yyyy%mm%dd')) AS svStusd1,
		{{ ref('ModNullHandling') }}.PL_APP_PROD_ID AS SRCE_KEY_I,
		'MAP_TYPE_C' AS CONV_M,
		'LOOKUP' AS CONV_MAP_RULE_M,
		'GRD_GNRC_MAP' AS TRSF_TABL_M,
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((InXfmBusinessRules.MAP_TYPE_C)) THEN (InXfmBusinessRules.MAP_TYPE_C) ELSE ""))) = 0) Then '  ' else InXfmBusinessRules.MAP_TYPE_C,
		IFF(LEN(TRIM(IFF({{ ref('ModNullHandling') }}.MAP_TYPE_C IS NOT NULL, {{ ref('ModNullHandling') }}.MAP_TYPE_C, ''))) = 0, '  ', {{ ref('ModNullHandling') }}.MAP_TYPE_C) AS VALU_CHNG_BFOR_X,
		{{ ref('ModNullHandling') }}.TARG_CHAR_C AS VALU_CHNG_AFTR_X,
		-- *SRC*: 'Job Name = ' : DSJobName,
		CONCAT('Job Name = ', DSJobName) AS TRSF_X,
		'FEAT_I' AS TRSF_COLM_M
	FROM {{ ref('ModNullHandling') }}
	WHERE svErrorCode2 = 'RPR1105'
)

SELECT * FROM XfmBusinessRules__OutErrorTargChar