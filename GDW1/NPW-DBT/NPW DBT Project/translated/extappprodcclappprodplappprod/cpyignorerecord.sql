{{ config(materialized='view', tags=['ExtAppProdCclAppProdPlAppProd']) }}

WITH CpyIgnoreRecord AS (
	SELECT
		
	FROM {{ ref('LuAppProdExcl') }}
)

SELECT * FROM CpyIgnoreRecord