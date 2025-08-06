{{ config(materialized='view', tags=['UtilProcessMetaDataApptOrigTC']) }}

WITH 
_dev_null AS (
	SELECT
	*
	FROM {{ source("","_dev_null")  }})
sq_ReadDataFiles AS (
	SELECT RECORD,
		FileName
	FROM _dev_null
)

SELECT * FROM sq_ReadDataFiles