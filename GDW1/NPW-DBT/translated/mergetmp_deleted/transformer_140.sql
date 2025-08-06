{{ config(materialized='view', tags=['MergeTMP_DELETED']) }}

WITH Transformer_140 AS (
	SELECT
		-- *SRC*: \(20)If Ld4.DELETED_TABLE_NAME = 'HL_APP_PROD' Then 'HL' Else ( If Ld4.DELETED_TABLE_NAME = 'PL_APP_PROD' Then 'PL' Else ( If Ld4.DELETED_TABLE_NAME = 'CCL_APP_PROD' Then 'CL' Else ( If Ld4.DELETED_TABLE_NAME = 'CC_APP_PROD' Then 'CC' Else ''))),
		IFF({{ ref('SrcAcctApptPdct') }}.DELETED_TABLE_NAME = 'HL_APP_PROD', 'HL', IFF({{ ref('SrcAcctApptPdct') }}.DELETED_TABLE_NAME = 'PL_APP_PROD', 'PL', IFF({{ ref('SrcAcctApptPdct') }}.DELETED_TABLE_NAME = 'CCL_APP_PROD', 'CL', IFF({{ ref('SrcAcctApptPdct') }}.DELETED_TABLE_NAME = 'CC_APP_PROD', 'CC', '')))) AS svStreamType,
		DELETED_TABLE_NAME,
		DELETED_KEY_1,
		-- *SRC*: 'CSE' : svStreamType : Ld4.DELETED_KEY_1_VALUE,
		CONCAT(CONCAT('CSE', svStreamType), {{ ref('SrcAcctApptPdct') }}.DELETED_KEY_1_VALUE) AS DELETED_KEY_1_VALUE,
		DELETED_KEY_2,
		DELETED_KEY_2_VALUE,
		DELETED_KEY_3,
		DELETED_KEY_3_VALUE,
		DELETED_KEY_4,
		DELETED_KEY_4_VALUE,
		DELETED_KEY_5,
		DELETED_KEY_5_VALUE
	FROM {{ ref('SrcAcctApptPdct') }}
	WHERE 
)

SELECT * FROM Transformer_140