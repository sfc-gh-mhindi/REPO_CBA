{{ config(materialized='view', tags=['XfmApptGnrcFile']) }}

WITH Remove_Duplicates AS (
	SELECT APPT_I, promise_type, {{ ref('Cpy') }}.MODF_S AS MODF_S_TEMP 
	FROM (
		SELECT APPT_I, promise_type, {{ ref('Cpy') }}.MODF_S AS MODF_S_TEMP,
		 ROW_NUMBER() OVER (PARTITION BY APPT_I, promise_type ORDER BY 1 DESC) AS ROW_NUM 
		FROM {{ ref('Cpy') }}
	) AS Remove_Duplicates_TEMP
	WHERE ROW_NUM = 1
)

SELECT * FROM Remove_Duplicates