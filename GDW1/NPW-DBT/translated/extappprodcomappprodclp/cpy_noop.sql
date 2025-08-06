{{ config(materialized='view', tags=['ExtAppProdComAppProdClp']) }}

WITH Cpy_NoOp AS (
	SELECT
		APP_PROD_ID,
		COM_SUBTYPE_CODE,
		CAMPAIGN_CAT_ID,
		COM_APP_ID
	FROM {{ ref('MergeDS') }}
)

SELECT * FROM Cpy_NoOp