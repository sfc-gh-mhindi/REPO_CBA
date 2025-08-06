WITH 
oldcopy as(
  select 
    OLD_APPT_I as NEW_APPT_I,
    OLD_DEPT_ROLE_C as NEW_DEPT_ROLE_C,
    OLD_DEPT_I as NEW_DEPT_I
  from {{ ref('cpyapptdeptentseq__dltappt_deptfrmtmp_appt_dept') }}
),
newcopy as(
  select 
    NEW_APPT_I,
    NEW_DEPT_ROLE_C,
    NEW_DEPT_I
  from {{ ref('cpyapptdeptentseq__dltappt_deptfrmtmp_appt_dept') }}
),

ChangeCapture AS (
  SELECT 
    COALESCE(old.NEW_APPT_I, new.NEW_APPT_I) AS NEW_APPT_I,
    COALESCE(old.NEW_DEPT_ROLE_C, new.NEW_DEPT_ROLE_C) AS NEW_DEPT_ROLE_C,
    COALESCE(old.NEW_DEPT_I, new.NEW_DEPT_I) AS NEW_DEPT_I,
    CASE 
      WHEN old.NEW_APPT_I IS NULL
        AND old.NEW_DEPT_ROLE_C IS NULL
        AND old.NEW_DEPT_I IS NULL
        THEN '1'
      WHEN new.NEW_APPT_I IS NULL
        AND new.NEW_DEPT_ROLE_C IS NULL
        AND new.NEW_DEPT_I IS NULL
        THEN '2'
      WHEN old.NEW_APPT_I = new.NEW_APPT_I
        AND old.NEW_DEPT_ROLE_C = new.NEW_DEPT_ROLE_C
        AND (
          old.NEW_DEPT_I <> new.NEW_DEPT_I
          OR (
            old.NEW_DEPT_I IS NULL
            AND new.NEW_DEPT_I IS NOT NULL
            )
          OR (
            old.NEW_DEPT_I IS NOT NULL
            AND new.NEW_DEPT_I IS NULL
            )
          )
        THEN '3'
      ELSE '0'
    END AS change_code

  FROM oldcopy old

  FULL JOIN newcopy new
  ON old.NEW_APPT_I = new.NEW_APPT_I AND old.NEW_DEPT_ROLE_C = new.NEW_DEPT_ROLE_C
)

SELECT 
  NEW_APPT_I,
  NEW_DEPT_ROLE_C,
  NEW_DEPT_I,
  change_code
FROM ChangeCapture