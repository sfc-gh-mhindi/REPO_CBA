{{ config(materialized='view', tags=['ExtComBusAppProdClientRole']) }}

WITH CpyNull AS (
	SELECT
		
	FROM {{ ref('LkpExclusions') }}
)

SELECT * FROM CpyNull