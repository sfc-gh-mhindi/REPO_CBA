{{ config(materialized='view', tags=['EXT_COM_BUS_SM_CASE_STATE']) }}

WITH Transformer_9 AS (
	SELECT
		RPT_ROW
	FROM {{ ref('Oracle_Enterprise_0') }}
	WHERE 
)

SELECT * FROM Transformer_9