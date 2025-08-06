{{ config(materialized='view', tags=['FileValidationApptPrmoTrakCSEL4']) }}

WITH xf_LastRecdNumb AS (
	SELECT
		-- *SRC*: Ln_Read_Recd_Cnt.ROW_NUMBER - 1,
		{{ ref('sq_SrceFileCount') }}.ROW_NUMBER - 1 AS ROW_NUMBER,
		FILE_NAME
	FROM {{ ref('sq_SrceFileCount') }}
	WHERE 
)

SELECT * FROM xf_LastRecdNumb