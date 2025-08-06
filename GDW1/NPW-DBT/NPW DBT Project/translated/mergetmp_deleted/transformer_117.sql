{{ config(materialized='view', tags=['MergeTMP_DELETED']) }}

WITH Transformer_117 AS (
	SELECT
		-- *SRC*: \(20)If InXfmBusinessRules.DELETED_TABLE_NAME = 'HL_APP_PROD' Then 'HL' Else ( If InXfmBusinessRules.DELETED_TABLE_NAME = 'PL_APP_PROD' Then 'PL' Else ( If InXfmBusinessRules.DELETED_TABLE_NAME = 'CCL_APP_PROD' Then 'CL' Else ( If InXfmBusinessRules.DELETED_TABLE_NAME = 'CC_APP_PROD' Then 'CC' Else ( If InXfmBusinessRules.DELETED_TABLE_NAME = 'APP_PROD_CLIENT_ROLE' Then InXfmBusinessRules.APPT_QLFY_C Else '')))),
		IFF(
	    {{ ref('ModNullHandling') }}.DELETED_TABLE_NAME = 'HL_APP_PROD', 'HL',     
	    IFF(
	        {{ ref('ModNullHandling') }}.DELETED_TABLE_NAME = 'PL_APP_PROD', 'PL', IFF({{ ref('ModNullHandling') }}.DELETED_TABLE_NAME = 'CCL_APP_PROD', 'CL', IFF({{ ref('ModNullHandling') }}.DELETED_TABLE_NAME = 'CC_APP_PROD', 'CC', IFF({{ ref('ModNullHandling') }}.DELETED_TABLE_NAME = 'APP_PROD_CLIENT_ROLE', {{ ref('ModNullHandling') }}.APPT_QLFY_C, '')))
	    )
	) AS svStreamType,
		-- *SRC*: \(20)If Len(Trim(( IF IsNotNull((InXfmBusinessRules.DELETED_KEY_3_VALUE)) THEN (InXfmBusinessRules.DELETED_KEY_3_VALUE) ELSE ""))) = 0 Then 'UNKN' Else InXfmBusinessRules.PATY_ROLE_C,
		IFF(LEN(TRIM(IFF({{ ref('ModNullHandling') }}.DELETED_KEY_3_VALUE IS NOT NULL, {{ ref('ModNullHandling') }}.DELETED_KEY_3_VALUE, ''))) = 0, 'UNKN', {{ ref('ModNullHandling') }}.PATY_ROLE_C) AS PATY_ROLE_C,
		DELETED_TABLE_NAME,
		DELETED_KEY_1,
		-- *SRC*: 'CSE' : svStreamType : InXfmBusinessRules.DELETED_KEY_1_VALUE,
		CONCAT(CONCAT('CSE', svStreamType), {{ ref('ModNullHandling') }}.DELETED_KEY_1_VALUE) AS DELETED_KEY_1_VALUE,
		DELETED_KEY_2,
		DELETED_KEY_2_VALUE,
		DELETED_KEY_3,
		DELETED_KEY_3_VALUE,
		DELETED_KEY_4,
		DELETED_KEY_4_VALUE,
		DELETED_KEY_5,
		DELETED_KEY_5_VALUE
	FROM {{ ref('ModNullHandling') }}
	WHERE 
)

SELECT * FROM Transformer_117