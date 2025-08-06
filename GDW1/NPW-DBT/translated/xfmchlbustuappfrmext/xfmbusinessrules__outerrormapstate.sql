{{ config(materialized='view', tags=['XfmChlBusTuAPPFrmExt']) }}

WITH XfmBusinessRules__OutErrorMapState AS (
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
		{{ ref('ModNullHandling') }}.TU_APP_ID AS SRCE_KEY_I,
		'STAT_C' AS CONV_M,
		'LOOKUP' AS CONV_MAP_RULE_M,
		'MAP_CSE_STATE' AS TRSF_TABL_M,
		{{ ref('ModNullHandling') }}.STAT_C AS VALU_CHNG_BFOR_X,
		'UNKNOWN' AS VALU_CHNG_AFTR_X,
		-- *SRC*: 'Job Name = ' : DSJobName,
		CONCAT('Job Name = ', DSJobName) AS TRSF_X,
		'STAT_X' AS TRSF_COLM_M
	FROM {{ ref('ModNullHandling') }}
	WHERE {{ ref('ModNullHandling') }}.STAT_C IS NOT NULL AND svErrorCodeState <> ''
)

SELECT * FROM XfmBusinessRules__OutErrorMapState