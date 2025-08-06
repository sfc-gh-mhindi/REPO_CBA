{{ config(materialized='view', tags=['XfmAppt_Empl']) }}

WITH Cpy AS (
	SELECT
		APP_ID,
		SUBTYPE_CODE,
		CREATED_BY_STAFF_NUMBER,
		OWNED_BY_STAFF_NUMBER
	FROM {{ ref('CSE_COM_BUS_APP') }}
)

SELECT * FROM Cpy