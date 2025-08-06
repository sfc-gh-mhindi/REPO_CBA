{{ config(materialized='view', tags=['FileValidationApptPrmoTrakCSEL4']) }}

WITH 
_dev_null AS (
	SELECT
	*
	FROM {{ source("","_dev_null")  }})
sq_SrceFileCount AS (
	SELECT ROW_NUMBER,
		FILE_NAME
	FROM _dev_null
)

SELECT * FROM sq_SrceFileCount