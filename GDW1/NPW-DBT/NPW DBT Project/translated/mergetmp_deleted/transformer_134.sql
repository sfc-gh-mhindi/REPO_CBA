{{ config(materialized='view', tags=['MergeTMP_DELETED']) }}

WITH Transformer_134 AS (
	SELECT
		DELETED_TABLE_NAME,
		DELETED_KEY_1,
		-- *SRC*: 'HR' : Ld2.DELETED_KEY_1_VALUE,
		CONCAT('HR', {{ ref('HlProdIntMarginRejtTableInsert') }}.DELETED_KEY_1_VALUE) AS DELETED_KEY_1_VALUE,
		DELETED_KEY_2,
		DELETED_KEY_2_VALUE,
		DELETED_KEY_3,
		DELETED_KEY_3_VALUE,
		DELETED_KEY_4,
		DELETED_KEY_4_VALUE,
		DELETED_KEY_5,
		DELETED_KEY_5_VALUE
	FROM {{ ref('HlProdIntMarginRejtTableInsert') }}
	WHERE 
)

SELECT * FROM Transformer_134