{{ config(materialized='view', tags=['XfmCclAppProdFrmExt1']) }}

WITH CpyNoOp AS (
	SELECT
		
	FROM {{ ref('LuAppProdExcl') }}
)

SELECT * FROM CpyNoOp