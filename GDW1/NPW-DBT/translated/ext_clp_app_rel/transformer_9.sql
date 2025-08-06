{{ config(materialized='view', tags=['EXT_CLP_APP_REL']) }}

WITH Transformer_9 AS (
	SELECT
		RPT_ROW
	FROM {{ ref('Oracle_Enterprise_0') }}
	WHERE 
)

SELECT * FROM Transformer_9