{{ config(materialized='view', tags=['LdDltAPP_ANS_EVNT_DEPT']) }}

WITH Copy AS (
	SELECT
		{{ ref('Src_Tera_APPT_PDCT_COND') }}.NEW_EVNT_I AS EVNT_I,
		{{ ref('Src_Tera_APPT_PDCT_COND') }}.NEW_DEPT_ROLE_C AS DEPT_ROLE_C,
		{{ ref('Src_Tera_APPT_PDCT_COND') }}.NEW_DEPT_I AS DEPT_I,
		{{ ref('Src_Tera_APPT_PDCT_COND') }}.OLD_EVNT_I AS EVNT_I,
		{{ ref('Src_Tera_APPT_PDCT_COND') }}.OLD_DEPT_ROLE_C AS DEPT_ROLE_C,
		{{ ref('Src_Tera_APPT_PDCT_COND') }}.OLD_DEPT_I AS DEPT_I
	FROM {{ ref('Src_Tera_APPT_PDCT_COND') }}
)

SELECT * FROM Copy