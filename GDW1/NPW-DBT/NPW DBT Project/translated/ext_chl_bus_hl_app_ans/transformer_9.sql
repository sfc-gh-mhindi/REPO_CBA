{{ config(materialized='view', tags=['EXT_CHL_BUS_HL_APP_ANS']) }}

WITH Transformer_9 AS (
	SELECT
		RPT_ROW
	FROM {{ ref('Oracle_Enterprise_0') }}
	WHERE 
)

SELECT * FROM Transformer_9