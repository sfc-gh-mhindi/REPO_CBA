
with 
before_sql as(
    select dummy from {{ ref('before__dltappt_deptfrmtmp_appt_dept') }}
    where 1=2
),
srctmpapptdepttera as(
SELECT 
    a.APPT_I AS NEW_APPT_I,
    a.DEPT_ROLE_C AS NEW_DEPT_ROLE_C,
    a.DEPT_I AS NEW_DEPT_I,
    b.APPT_I AS OLD_APPT_I,
    b.DEPT_ROLE_C AS OLD_DEPT_ROLE_C,
    b.DEPT_I AS OLD_DEPT_I,
    b.EFFT_D AS OLD_EFFT_D
 
FROM {{ cvar('stg_ctl_db') }}.{{ cvar("gdw_stag_db") }}.TMP_APPT_DEPT a
LEFT OUTER JOIN {{ cvar('stg_ctl_db') }}.{{ cvar("gdw_acct_vw") }}.APPT_DEPT b
ON trim(a.APPT_I) = trim(b.APPT_I) AND trim(a.DEPT_ROLE_C) = trim(b.DEPT_ROLE_C) AND b.EXPY_D='9999-12-31'
WHERE a.RUN_STRM = '{{ cvar("run_stream") }}'
)

SELECT
    NEW_APPT_I,
    NEW_DEPT_ROLE_C,
    NEW_DEPT_I,
    OLD_APPT_I,
    OLD_DEPT_ROLE_C,
    OLD_DEPT_I,
    OLD_EFFT_D
from srctmpapptdepttera
