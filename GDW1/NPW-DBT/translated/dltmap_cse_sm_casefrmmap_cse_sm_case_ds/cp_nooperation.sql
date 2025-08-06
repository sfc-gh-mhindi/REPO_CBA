{{ config(materialized='view', tags=['DltMAP_CSE_SM_CASEFrmMAP_CSE_SM_CASE_DS']) }}

WITH Cp_NoOperation AS (
	SELECT
		
	FROM {{ ref('LuSmCaseId') }}
)

SELECT * FROM Cp_NoOperation