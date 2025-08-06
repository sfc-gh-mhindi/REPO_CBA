{{ config(materialized='view', tags=['UtilProcessMetaDataTC_Promo']) }}

WITH xf_Transform AS (
	SELECT
		pGDW_PROS_ID AS SyncId,
		-- *SRC*: \(20)If Ln_ReadData.FileName[5][1, 1] = 'I' Then Ln_ReadData.RECORD Else 0,
		IFF(SUBSTRING(SUBSTRING({{ ref('sq_ReadDataFiles') }}.FileName, 5), 1, 1) = 'I', {{ ref('sq_ReadDataFiles') }}.RECORD, 0) AS Insert,
		-- *SRC*: \(20)If Ln_ReadData.FileName[5][1, 1] = 'U' Then Ln_ReadData.RECORD Else 0,
		IFF(SUBSTRING(SUBSTRING({{ ref('sq_ReadDataFiles') }}.FileName, 5), 1, 1) = 'U', {{ ref('sq_ReadDataFiles') }}.RECORD, 0) AS Update,
		-- *SRC*: \(20)If Ln_ReadData.FileName[5][1, 1] = 'D' Then Ln_ReadData.RECORD Else 0,
		IFF(SUBSTRING(SUBSTRING({{ ref('sq_ReadDataFiles') }}.FileName, 5), 1, 1) = 'D', {{ ref('sq_ReadDataFiles') }}.RECORD, 0) AS Delete
	FROM {{ ref('sq_ReadDataFiles') }}
	WHERE 
)

SELECT * FROM xf_Transform