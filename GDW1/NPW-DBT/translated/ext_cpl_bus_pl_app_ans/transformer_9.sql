{{ config(materialized='view', tags=['EXT_CPL_BUS_PL_APP_ANS']) }}

WITH Transformer_9 AS (
	SELECT
		RPT_ROW
	FROM {{ ref('Oracle_Enterprise_0') }}
	WHERE 
)

SELECT * FROM Transformer_9