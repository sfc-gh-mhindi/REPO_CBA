{{ config(materialized='view', tags=['XfmTu_ApptPdctFnddInssFrmExt']) }}

WITH XfmBusinessRules__OutTmpAppPdctFnddInssDS AS (
	SELECT
		-- *SRC*: 'CSE' : InXfmBusinessRules.APPT_QLFY_C : InXfmBusinessRules.HL_APP_PROD_ID,
		CONCAT(CONCAT('CSE', {{ ref('ModNullHandling') }}.APPT_QLFY_C), {{ ref('ModNullHandling') }}.HL_APP_PROD_ID) AS APPT_PDCT_I,
		-- *SRC*: 'CSE' : InXfmBusinessRules.APPT_QLFY_C : InXfmBusinessRules.TU_APP_FUNDING_ID,
		CONCAT(CONCAT('CSE', {{ ref('ModNullHandling') }}.APPT_QLFY_C), {{ ref('ModNullHandling') }}.TU_APP_FUNDING_ID) AS APPT_PDCT_FNDD_I,
		-- *SRC*: 'CSE' : InXfmBusinessRules.APPT_QLFY_C : InXfmBusinessRules.TU_APP_FUNDING_DETAIL_ID,
		CONCAT(CONCAT('CSE', {{ ref('ModNullHandling') }}.APPT_QLFY_C), {{ ref('ModNullHandling') }}.TU_APP_FUNDING_DETAIL_ID) AS APPT_PDCT_FNDD_METH_I,
		-- *SRC*: StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd'),
		STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd') AS EFFT_D,
		-- *SRC*: \(20)If IsNull(InXfmBusinessRules.FUNDING_CBA_ACCOUNT_ID) and IsNull(InXfmBusinessRules.FUNDING_METHOD_CAT_ID) Then '' else  if IsNotNull(InXfmBusinessRules.FUNDING_CBA_ACCOUNT_ID) and IsNull(InXfmBusinessRules.FUNDING_METHOD_CAT_ID) Then 'CBA' else InXfmBusinessRules.FNDD_INSS_METH_C,
		IFF({{ ref('ModNullHandling') }}.FUNDING_CBA_ACCOUNT_ID IS NULL AND {{ ref('ModNullHandling') }}.FUNDING_METHOD_CAT_ID IS NULL, '', IFF({{ ref('ModNullHandling') }}.FUNDING_CBA_ACCOUNT_ID IS NOT NULL AND {{ ref('ModNullHandling') }}.FUNDING_METHOD_CAT_ID IS NULL, 'CBA', {{ ref('ModNullHandling') }}.FNDD_INSS_METH_C)) AS FNDD_INSS_METH_C,
		{{ ref('ModNullHandling') }}.TU_APP_FUNDING_ID AS SRCE_SYST_FNDD_I,
		{{ ref('ModNullHandling') }}.TU_APP_FUNDING_DETAIL_ID AS SRCE_SYST_FNDD_METH_I,
		'CSE' AS SRCE_SYST_C,
		-- *SRC*: \(20)If IsNull(InXfmBusinessRules.FUNDING_DATE) Then setnull() Else  If (Len(Trim(( IF IsNotNull((InXfmBusinessRules.FUNDING_DATE)) THEN (InXfmBusinessRules.FUNDING_DATE) ELSE ""))) <> 8) Then StringToDate('11111111', '%yyyy%mm%dd') Else StringToDate(InXfmBusinessRules.FUNDING_DATE, '%yyyy%mm%dd'),
		IFF({{ ref('ModNullHandling') }}.FUNDING_DATE IS NULL, SETNULL(), IFF(LEN(TRIM(IFF({{ ref('ModNullHandling') }}.FUNDING_DATE IS NOT NULL, {{ ref('ModNullHandling') }}.FUNDING_DATE, ''))) <> 8, STRINGTODATE('11111111', '%yyyy%mm%dd'), STRINGTODATE({{ ref('ModNullHandling') }}.FUNDING_DATE, '%yyyy%mm%dd'))) AS FNDD_D,
		{{ ref('ModNullHandling') }}.PROGRESSIVE_PAYMENT_AMT AS FNDD_A,
		{{ ref('ModNullHandling') }}.FUNDING_CBA_ACCOUNT_ID AS PDCT_SYST_ACCT_N,
		-- *SRC*: \(20)If IsNull(InXfmBusinessRules.FUNDING_NONCBA_BANK_NUMBER) Then '' ELSE InXfmBusinessRules.CMPE_I,
		IFF({{ ref('ModNullHandling') }}.FUNDING_NONCBA_BANK_NUMBER IS NULL, '', {{ ref('ModNullHandling') }}.CMPE_I) AS CMPE_I,
		{{ ref('ModNullHandling') }}.FUNDING_NONCBA_BSB AS CMPE_ACCT_BSB_N,
		{{ ref('ModNullHandling') }}.FUNDING_NONCBA_ACCOUNT_NUMBER AS CMPE_ACCT_N,
		-- *SRC*: StringToDate('9999-12-31', '%yyyy-%mm-%dd'),
		STRINGTODATE('9999-12-31', '%yyyy-%mm-%dd') AS EXPY_D,
		0 AS PROS_KEY_EFFT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS PROS_KEY_EXPY_I,
		-- *SRC*: SetNull(),
		SETNULL() AS EROR_SEQN_I,
		RUN_STREAM AS RUN_STREAM
	FROM {{ ref('ModNullHandling') }}
	WHERE 
)

SELECT * FROM XfmBusinessRules__OutTmpAppPdctFnddInssDS