{{ config(materialized='view', tags=['MergeTMP_DELETED']) }}

WITH Transformer_130 AS (
	SELECT
		DELETED_TABLE_NAME,
		DELETED_KEY_1,
		-- *SRC*: 'HF' : Ld1.DELETED_KEY_1_VALUE,
		CONCAT('HF', {{ ref('HlFeeHlFeeDiscountRejtTableInsert') }}.DELETED_KEY_1_VALUE) AS DELETED_KEY_1_VALUE,
		DELETED_KEY_2,
		DELETED_KEY_2_VALUE,
		DELETED_KEY_3,
		DELETED_KEY_3_VALUE,
		DELETED_KEY_4,
		DELETED_KEY_4_VALUE,
		DELETED_KEY_5,
		DELETED_KEY_5_VALUE
	FROM {{ ref('HlFeeHlFeeDiscountRejtTableInsert') }}
	WHERE 
)

SELECT * FROM Transformer_130