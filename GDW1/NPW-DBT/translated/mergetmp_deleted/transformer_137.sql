{{ config(materialized='view', tags=['MergeTMP_DELETED']) }}

WITH Transformer_137 AS (
	SELECT
		DELETED_TABLE_NAME,
		DELETED_KEY_1,
		-- *SRC*: 'PR' : Ld3.DELETED_KEY_1_VALUE,
		CONCAT('PR', {{ ref('PlIntRatePlMarginRejtTableInsert') }}.DELETED_KEY_1_VALUE) AS DELETED_KEY_1_VALUE,
		DELETED_KEY_2,
		DELETED_KEY_2_VALUE,
		DELETED_KEY_3,
		DELETED_KEY_3_VALUE,
		DELETED_KEY_4,
		DELETED_KEY_4_VALUE,
		DELETED_KEY_5,
		DELETED_KEY_5_VALUE
	FROM {{ ref('PlIntRatePlMarginRejtTableInsert') }}
	WHERE 
)

SELECT * FROM Transformer_137