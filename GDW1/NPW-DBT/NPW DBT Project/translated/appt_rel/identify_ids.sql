{{ config(materialized='view', tags=['APPT_REL']) }}

WITH Identify_IDs AS (
	SELECT
		APP_ID,
		SUBTYPE_CODE,
		LOAN_APP_ID,
		LOAN_SUBTYPE_CODE
	FROM {{ ref('Sequential_File_48') }}
	WHERE 
)

SELECT * FROM Identify_IDs