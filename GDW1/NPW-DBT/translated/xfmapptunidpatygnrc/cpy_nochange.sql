{{ config(materialized='view', tags=['XfmApptUnidPatyGnrc']) }}

WITH Cpy_NoChange AS (
	SELECT
		
	FROM {{ ref('Lk_BusRules') }}
)

SELECT * FROM Cpy_NoChange