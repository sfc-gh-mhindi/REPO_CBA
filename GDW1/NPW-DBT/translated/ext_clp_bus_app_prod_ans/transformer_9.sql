{{ config(materialized='view', tags=['EXT_CLP_BUS_APP_PROD_ANS']) }}

WITH Transformer_9 AS (
	SELECT
		RPT_ROW
	FROM {{ ref('Oracle_Enterprise_0') }}
	WHERE 
)

SELECT * FROM Transformer_9