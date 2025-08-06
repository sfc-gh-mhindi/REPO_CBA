{{ config(materialized='view', tags=['XfmAppt_Dept']) }}

WITH Cpy AS (
	SELECT
		APP_ID,
		SUBTYPE_CODE,
		GL_DEPT_NO
	FROM {{ ref('CSE_COM_BUS_APP') }}
)

SELECT * FROM Cpy