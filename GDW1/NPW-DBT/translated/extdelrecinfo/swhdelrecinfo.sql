{{ config(materialized='view', tags=['ExtDelRecInfo']) }}

WITH --Manual Task - PxSwitch - SwhDelRecInfo

SELECT * FROM SwhDelRecInfo