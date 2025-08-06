{{ config(materialized='view', tags=['XfmChlBusPrtyAdrs']) }}

WITH XfmBusinessRules__OutAdrsDS AS (
	SELECT
		-- *SRC*: \(20)if (len(trim(( IF IsNotNull((InXfmBusinessRules.CHL_ADDRESS_LINE_1)) THEN (InXfmBusinessRules.CHL_ADDRESS_LINE_1) ELSE ""))) <> 0 or len(trim(( IF IsNotNull((InXfmBusinessRules.CHL_ADDRESS_LINE_2)) THEN (InXfmBusinessRules.CHL_ADDRESS_LINE_2) ELSE ""))) <> 0) and (len(trim(( IF IsNotNull((InXfmBusinessRules.CHL_SUBURB)) THEN (InXfmBusinessRules.CHL_SUBURB) ELSE ""))) <> 0 or len(trim(( IF IsNotNull((InXfmBusinessRules.CHL_POSTCODE)) THEN (InXfmBusinessRules.CHL_POSTCODE) ELSE ""))) <> 0 or len(trim(( IF IsNotNull((InXfmBusinessRules.CHL_STATE)) THEN (InXfmBusinessRules.CHL_STATE) ELSE ""))) <> 0) then 'Y' else 'N',
		IFF(    
	    LEN(TRIM(IFF({{ ref('ModNullHandling') }}.CHL_ADDRESS_LINE_1 IS NOT NULL, {{ ref('ModNullHandling') }}.CHL_ADDRESS_LINE_1, ''))) <> 0
	    or LEN(TRIM(IFF({{ ref('ModNullHandling') }}.CHL_ADDRESS_LINE_2 IS NOT NULL, {{ ref('ModNullHandling') }}.CHL_ADDRESS_LINE_2, ''))) <> 0
	    and LEN(TRIM(IFF({{ ref('ModNullHandling') }}.CHL_SUBURB IS NOT NULL, {{ ref('ModNullHandling') }}.CHL_SUBURB, ''))) <> 0
	    or LEN(TRIM(IFF({{ ref('ModNullHandling') }}.CHL_POSTCODE IS NOT NULL, {{ ref('ModNullHandling') }}.CHL_POSTCODE, ''))) <> 0
	    or LEN(TRIM(IFF({{ ref('ModNullHandling') }}.CHL_STATE IS NOT NULL, {{ ref('ModNullHandling') }}.CHL_STATE, ''))) <> 0, 
	    'Y', 
	    'N'
	) AS LoadAdrs,
		-- *SRC*: \(20)If InXfmBusinessRules.APPT_QLFY_C = '99' Then 'RPR8001' Else '',
		IFF({{ ref('ModNullHandling') }}.APPT_QLFY_C = '99', 'RPR8001', '') AS ErrorCode_1,
		-- *SRC*: \(20)If isnotnull(InXfmBusinessRules.CHL_COUNTRY_ID) and InXfmBusinessRules.ISO_CNTY_C = '99' then 'RPR8002' Else '',
		IFF({{ ref('ModNullHandling') }}.CHL_COUNTRY_ID IS NOT NULL AND {{ ref('ModNullHandling') }}.ISO_CNTY_C = '99', 'RPR8002', '') AS ErrorCode_2,
		-- *SRC*: \(20)If ErrorCode_1 <> "" Then ErrorCode_1 Else  If ErrorCode_2 <> "" Then ErrorCode_2 Else '',
		IFF(ErrorCode_1 <> '', ErrorCode_1, IFF(ErrorCode_2 <> '', ErrorCode_2, '')) AS ErrorCode,
		-- *SRC*: \(20)If ErrorCode <> "" Then @TRUE Else @FALSE,
		IFF(ErrorCode <> '', @TRUE, @FALSE) AS RejectFlag,
		-- *SRC*: 'CSEPAC1' : InXfmBusinessRules.CHL_ASSET_LIABILITY_ID,
		CONCAT('CSEPAC1', {{ ref('ModNullHandling') }}.CHL_ASSET_LIABILITY_ID) AS ADRS_I,
		'P' AS ADRS_TYPE_C,
		'CSE' AS SRCE_SYST_C,
		'PA' AS ADRS_QLFY_C,
		{{ ref('ModNullHandling') }}.CHL_ASSET_LIABILITY_ID AS SRCE_SYST_ADRS_I,
		-- *SRC*: setnull(),
		SETNULL() AS SRCE_SYST_ADRS_SEQN_N,
		RUN_STREAM AS RUN_STRM
	FROM {{ ref('ModNullHandling') }}
	WHERE LoadAdrs = 'Y' AND {{ ref('ModNullHandling') }}.CHL_PRINCIPAL_SECURITY_FLAG = 'Y'
)

SELECT * FROM XfmBusinessRules__OutAdrsDS