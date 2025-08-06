{{ config(materialized='view', tags=['DltAppt_DeptFrmTMP_XFM']) }}

WITH RmdpFrmInput AS (
	SELECT NEW_APPT_I, NEW_DEPT_ROLE_C, NEW_DEPT_I, OLD_EFFT_D, change_code, OLD_DEPT_I 
	FROM (
		SELECT NEW_APPT_I, NEW_DEPT_ROLE_C, NEW_DEPT_I, OLD_EFFT_D, change_code, OLD_DEPT_I,
		 ROW_NUMBER() OVER (PARTITION BY NEW_APPT_I, NEW_DEPT_ROLE_C, NEW_DEPT_I ORDER BY 1 ASC) AS ROW_NUM 
		FROM {{ ref('Join') }}
	) AS RmdpFrmInput_TEMP
	WHERE ROW_NUM = 1
)

SELECT * FROM RmdpFrmInput