{{ config(materialized='view', tags=['XfmChlBusTuAPPFrmExt']) }}

WITH XfmBusinessRules__OutTmpApptDelyInssDS AS (
	SELECT
		-- *SRC*: \(20)If InXfmBusinessRules.TU_DOCCOLLECT_METHOD_CAT_ID = 'NA' Then @FALSE else @TRUE,
		IFF({{ ref('ModNullHandling') }}.TU_DOCCOLLECT_METHOD_CAT_ID = 'NA', @FALSE, @TRUE) AS svLoadDelyInss,
		-- *SRC*: \(20)If trim(InXfmBusinessRules.CRIS_PRODUCT_ID) = 'NA' or trim(InXfmBusinessRules.ACCOUNT_NO) = 'NA' or InXfmBusinessRules.SRCE_SYST_C = 'NA' Then @FALSE Else @TRUE,
		IFF(TRIM({{ ref('ModNullHandling') }}.CRIS_PRODUCT_ID) = 'NA' OR TRIM({{ ref('ModNullHandling') }}.ACCOUNT_NO) = 'NA' OR {{ ref('ModNullHandling') }}.SRCE_SYST_C = 'NA', @FALSE, @TRUE) AS svLoadApptPdctAcct,
		-- *SRC*: \(20)If InXfmBusinessRules.TOPUP_AGENT_ID <> 'NA' Then @TRUE Else @FALSE,
		IFF({{ ref('ModNullHandling') }}.TOPUP_AGENT_ID <> 'NA', @TRUE, @FALSE) AS svLoadUnidPaty,
		-- *SRC*: \(20)if InXfmBusinessRules.DOCU_DELY_METH_C = '99999' and InXfmBusinessRules.TU_DOCCOLLECT_METHOD_CAT_ID = 'NA' then 'REJ7101' else '',
		IFF({{ ref('ModNullHandling') }}.DOCU_DELY_METH_C = '99999' AND {{ ref('ModNullHandling') }}.TU_DOCCOLLECT_METHOD_CAT_ID = 'NA', 'REJ7101', '') AS svErrorCodeDelyMeth,
		-- *SRC*: \(20)if isnotnull(InXfmBusinessRules.ADRS_TYPE_ID) and InXfmBusinessRules.PYAD_TYPE_C = '99999' then 'REJ7102' else '',
		IFF({{ ref('ModNullHandling') }}.ADRS_TYPE_ID IS NOT NULL AND {{ ref('ModNullHandling') }}.PYAD_TYPE_C = '99999', 'REJ7102', '') AS svErrorCodeAddrType,
		-- *SRC*: \(20)if InXfmBusinessRules.TU_DOCCOLLECT_OVERSEA_STATE = '-1' and trim(InXfmBusinessRules.STAT_X) = 'UNKNOWN' then 'REJ7103' else '',
		IFF({{ ref('ModNullHandling') }}.TU_DOCCOLLECT_OVERSEA_STATE = '-1' AND TRIM({{ ref('ModNullHandling') }}.STAT_X) = 'UNKNOWN', 'REJ7103', '') AS svErrorCodeState,
		-- *SRC*: \(20)if isnotnull(InXfmBusinessRules.CNTY_ID) and InXfmBusinessRules.ISO_CNTY_C = '99' then 'REJ7104' else '',
		IFF({{ ref('ModNullHandling') }}.CNTY_ID IS NOT NULL AND {{ ref('ModNullHandling') }}.ISO_CNTY_C = '99', 'REJ7104', '') AS svErrorCodeCnty,
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((InXfmBusinessRules.CAMPAIGN_CODE)) THEN (InXfmBusinessRules.CAMPAIGN_CODE) ELSE ""))) = 0) Then 0 ELSE 1,
		IFF(LEN(TRIM(IFF({{ ref('ModNullHandling') }}.CAMPAIGN_CODE IS NOT NULL, {{ ref('ModNullHandling') }}.CAMPAIGN_CODE, ''))) = 0, 0, 1) AS svCampaignCode,
		-- *SRC*: \(20)If Trim(Trim(InXfmBusinessRules.TU_APP_ID, '0')) = '' Then 'N' Else 'Y',
		IFF(TRIM(TRIM({{ ref('ModNullHandling') }}.TU_APP_ID, '0')) = '', 'N', 'Y') AS svTuAppI,
		-- *SRC*: 'CSE' : InXfmBusinessRules.APPT_QLFY_C : InXfmBusinessRules.HL_APP_PROD_ID,
		CONCAT(CONCAT('CSE', {{ ref('ModNullHandling') }}.APPT_QLFY_C), {{ ref('ModNullHandling') }}.HL_APP_PROD_ID) AS APPT_PDCT_I,
		-- *SRC*: StringToDate(ETL_PROCESS_DT, "%yyyy%mm%dd"),
		STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd') AS EFFT_D,
		DOCU_DELY_METH_C,
		-- *SRC*: \(20)if isnull(InXfmBusinessRules.ADRS_TYPE_ID) then setnull() else InXfmBusinessRules.PYAD_TYPE_C,
		IFF({{ ref('ModNullHandling') }}.ADRS_TYPE_ID IS NULL, SETNULL(), {{ ref('ModNullHandling') }}.PYAD_TYPE_C) AS PYAD_TYPE_C,
		{{ ref('ModNullHandling') }}.TU_DOCCOLLECT_ADDRESS_LINE_1 AS ADRS_LINE_1_X,
		{{ ref('ModNullHandling') }}.TU_DOCCOLLECT_ADDRESS_LINE_2 AS ADRS_LINE_2_X,
		-- *SRC*: \(20)if trim(InXfmBusinessRules.TU_DOCCOLLECT_SUBURB) = '-1' then setnull() else InXfmBusinessRules.TU_DOCCOLLECT_SUBURB,
		IFF(TRIM({{ ref('ModNullHandling') }}.TU_DOCCOLLECT_SUBURB) = '-1', SETNULL(), {{ ref('ModNullHandling') }}.TU_DOCCOLLECT_SUBURB) AS SURB_X,
		{{ ref('ModNullHandling') }}.TU_DOCCOLLECT_POSTCODE AS PCOD_C,
		-- *SRC*: \(20)if InXfmBusinessRules.TU_DOCCOLLECT_OVERSEA_STATE <> '-1' then InXfmBusinessRules.TU_DOCCOLLECT_OVERSEA_STATE else  if isnull(InXfmBusinessRules.STAT_C) then setnull() else InXfmBusinessRules.STAT_X,
		IFF({{ ref('ModNullHandling') }}.TU_DOCCOLLECT_OVERSEA_STATE <> '-1', {{ ref('ModNullHandling') }}.TU_DOCCOLLECT_OVERSEA_STATE, IFF({{ ref('ModNullHandling') }}.STAT_C IS NULL, SETNULL(), {{ ref('ModNullHandling') }}.STAT_X)) AS STAT_X,
		-- *SRC*: \(20)if isnull(InXfmBusinessRules.CNTY_ID) then setnull() else InXfmBusinessRules.ISO_CNTY_C,
		IFF({{ ref('ModNullHandling') }}.CNTY_ID IS NULL, SETNULL(), {{ ref('ModNullHandling') }}.ISO_CNTY_C) AS ISO_CNTY_C,
		-- *SRC*: \(20)if (InXfmBusinessRules.DOCCOLLECT_BSB) = 'NA' then setnull() else right(trim(InXfmBusinessRules.DOCCOLLECT_BSB), 4),
		IFF({{ ref('ModNullHandling') }}.DOCCOLLECT_BSB = 'NA', SETNULL(), RIGHT(TRIM({{ ref('ModNullHandling') }}.DOCCOLLECT_BSB), 4)) AS BRCH_N,
		'9999-12-31' AS EXPY_D,
		0 AS PROS_KEY_EFFT_I,
		-- *SRC*: setnull(),
		SETNULL() AS PROS_KEY_EXPY_I,
		-- *SRC*: setnull(),
		SETNULL() AS EROR_SEQN_I,
		RUN_STREAM AS RUN_STRM
	FROM {{ ref('ModNullHandling') }}
	WHERE svLoadDelyInss
)

SELECT * FROM XfmBusinessRules__OutTmpApptDelyInssDS