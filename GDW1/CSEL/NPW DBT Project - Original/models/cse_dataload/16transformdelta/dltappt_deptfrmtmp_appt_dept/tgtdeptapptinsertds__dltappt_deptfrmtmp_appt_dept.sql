{{
  config(
    post_hook=[
      'INSERT overwrite INTO ' ~ cvar("intermediate_db") ~ '.' ~ cvar("datasets_schema") ~ '.'~ cvar("base_dir") ~ '__dataset__DEPT_APPT_I_'~cvar("run_stream")~'_'~ cvar("etl_process_dt") ~'__DS SELECT * FROM {{ this }}'
    ]
  )
}}

WITH xfmcheckdeltaaction AS (
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
  FROM {{ ref('xfmcheckdeltaaction__dltappt_deptfrmtmp_appt_dept') }}
),

tgtdeptapptinsertds as(
  select
    COALESCE(NEW_APPT_I, '') AS APPT_I,
    COALESCE(NEW_DEPT_ROLE_C, '') AS DEPT_ROLE_C,
    TO_DATE('{{ cvar("etl_process_dt") }}', 'yyyymmdd') AS EFFT_D,
    NEW_DEPT_I AS DEPT_I,
    TO_DATE('9999-12-31', 'yyyy-mm-dd') AS EXPY_D,
    {{ cvar("refr_pk") }} AS PROS_KEY_EFFT_I,
    NULL AS PROS_KEY_EXPY_I,
    NULL AS EROR_SEQN_I
  from xfmcheckdeltaaction
  where change_code = "INSERT" or change_code = "UPDATE"
)

SELECT
  APPT_I,
  DEPT_ROLE_C,
  EFFT_D,
  DEPT_I,
  EXPY_D,
  PROS_KEY_EFFT_I,
  PROS_KEY_EXPY_I,
  EROR_SEQN_I
FROM tgtdeptapptinsertds