{{ config(materialized='view', tags=['FileValidationApptPrmoTrakCSEL4']) }}

WITH --Manual Task - None - WriteToLog

SELECT * FROM WriteToLog