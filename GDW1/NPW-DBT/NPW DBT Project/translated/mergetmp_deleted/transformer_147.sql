{{ config(materialized='view', tags=['MergeTMP_DELETED']) }}

WITH Transformer_147 AS (
	SELECT
		-- *SRC*: \(20)If Ld9.DELETED_TABLE_NAME = 'HL_APP_PROD' Then 'HL' Else ( If Ld9.DELETED_TABLE_NAME = 'PL_APP_PROD' Then 'PL' Else ( If Ld9.DELETED_TABLE_NAME = 'CCL_APP_PROD' Then 'CL' Else '')),
		IFF({{ ref('SrcApptPatyRel') }}.DELETED_TABLE_NAME = 'HL_APP_PROD', 'HL', IFF({{ ref('SrcApptPatyRel') }}.DELETED_TABLE_NAME = 'PL_APP_PROD', 'PL', IFF({{ ref('SrcApptPatyRel') }}.DELETED_TABLE_NAME = 'CCL_APP_PROD', 'CL', ''))) AS svStreamType,
		DELETED_TABLE_NAME,
		DELETED_KEY_1,
		-- *SRC*: 'CSE' : svStreamType : Ld9.DELETED_KEY_1_VALUE,
		CONCAT(CONCAT('CSE', svStreamType), {{ ref('SrcApptPatyRel') }}.DELETED_KEY_1_VALUE) AS DELETED_KEY_1_VALUE,
		DELETED_KEY_2,
		DELETED_KEY_2_VALUE,
		DELETED_KEY_3,
		DELETED_KEY_3_VALUE,
		DELETED_KEY_4,
		DELETED_KEY_4_VALUE,
		DELETED_KEY_5,
		DELETED_KEY_5_VALUE
	FROM {{ ref('SrcApptPatyRel') }}
	WHERE 
)

SELECT * FROM Transformer_147