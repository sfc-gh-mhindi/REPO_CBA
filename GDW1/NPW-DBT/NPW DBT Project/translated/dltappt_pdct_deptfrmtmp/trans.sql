{{ config(materialized='view', tags=['DltAppt_Pdct_DeptFrmTMP']) }}

WITH Trans AS (
	SELECT
		APPT_PDCT_I,
		-- *SRC*: Trim(Appt_Qstn_Tgt.BRCH_N),
		TRIM({{ ref('Appt_Pdct_Dept_Tgt') }}.BRCH_N) AS BRCH_N,
		DEPT_I,
		DEPT_ROLE_C,
		SRCE_SYST_C,
		EFFT_D
	FROM {{ ref('Appt_Pdct_Dept_Tgt') }}
	WHERE 
)

SELECT * FROM Trans