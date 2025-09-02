WITH join__dltappt_deptfrmtmp_appt_dept as(
    select 
        NEW_APPT_I,
        NEW_DEPT_ROLE_C,
        NEW_DEPT_I,
        OLD_EFFT_D,
        change_code,
        OLD_DEPT_I
    from {{ ref('join__dltappt_deptfrmtmp_appt_dept') }}
),

xfmcheckdeltaaction AS (
	SELECT
        NEW_APPT_I,
        NEW_DEPT_ROLE_C,
        NEW_DEPT_I,
        OLD_EFFT_D,
        change_code,
        OLD_DEPT_I,
        DATEADD(DAY, -1, TO_DATE('{{ cvar("etl_process_dt") }}', 'YYYYMMDD')) AS ExpiryDate,
		1 AS "INSERT",
		3 AS "UPDATE"
	FROM join__dltappt_deptfrmtmp_appt_dept
)

SELECT 
    NEW_APPT_I,
    NEW_DEPT_ROLE_C,
    NEW_DEPT_I,
    OLD_EFFT_D,
    change_code,
    OLD_DEPT_I,
    ExpiryDate,
    "INSERT",
    "UPDATE"
FROM xfmcheckdeltaaction