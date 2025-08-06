{{ config(materialized='view', tags=['ValidateBcFinsg']) }}

WITH --Manual Task - None - WriteToLog

SELECT * FROM WriteToLog