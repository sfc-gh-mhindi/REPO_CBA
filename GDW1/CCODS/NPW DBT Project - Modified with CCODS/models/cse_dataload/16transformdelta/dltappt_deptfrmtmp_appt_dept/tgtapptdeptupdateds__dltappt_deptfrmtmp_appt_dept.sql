{{
  config(
    post_hook=[
      'INSERT overwrite INTO ' ~ cvar("intermediate_db") ~ '.' ~ cvar("datasets_schema") ~ '.'~ cvar("base_dir") ~ '__dataset__'~ cvar("tgt_table")~ '_U_'~ cvar("run_stream") ~'_'~ cvar("etl_process_dt") ~'__DS SELECT * FROM {{ this }}'
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

tgtdeptapptupdateds as(
  select
    OLD_DEPT_I AS DEPT_I,
    COALESCE(NEW_APPT_I, '') AS APPT_I,
    COALESCE(NEW_DEPT_ROLE_C, '') AS DEPT_ROLE_C,
    COALESCE(OLD_EFFT_D, DATE '1900-01-01') AS EFFT_D,
    ExpiryDate AS EXPY_D,
    {{ cvar("refr_pk") }} AS PROS_KEY_EXPY_I
  from xfmcheckdeltaaction
  where change_code = "UPDATE"
)

SELECT
  DEPT_I,
  APPT_I,
  DEPT_ROLE_C,
  EFFT_D,
  EXPY_D,
  PROS_KEY_EXPY_I
FROM tgtdeptapptupdateds