{{ config(materialized='view', tags=['UtilProcessMetaDataTC_Promo']) }}

WITH ag_CntTypes AS (
	SELECT
		SyncId,
		-- *SRC*: Sum(Ln_CntTypes.Insert),
		SUM({{ ref('xf_Transform') }}.Insert) AS InsertCount,
		-- *SRC*: Sum(Ln_CntTypes.Update),
		SUM({{ ref('xf_Transform') }}.Update) AS UpdateCount,
		-- *SRC*: Sum(Ln_CntTypes.Delete),
		SUM({{ ref('xf_Transform') }}.Delete) AS DeleteCount 
	FROM {{ ref('xf_Transform') }}
	GROUP BY SyncId
)

SELECT * FROM ag_CntTypes