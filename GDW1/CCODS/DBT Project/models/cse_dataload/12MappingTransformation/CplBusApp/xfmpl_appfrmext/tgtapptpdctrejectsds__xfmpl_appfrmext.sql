{{
  config(
    post_hook=[
      'INSERT overwrite INTO '~cvar("intermediate_db")~'.'~ cvar("datasets_schema") ~ '.'~ cvar("base_dir") ~'__dataset__'~ cvar("run_stream") ~ '_Mapping_Rejects__DS SELECT * FROM {{ this }}'
    ]
  )
}}

with tgtapptpdctrejectsds as (
select
    PL_APP_ID,
    NOMINATED_BRANCH_ID,
    PL_PACKAGE_CAT_ID,
    '{{ cvar("etl_process_dt") }}' as ETL_D,
    ORIG_ETL_D,
    svErrorCode as EROR_C
from {{ ref('xfmbusinessrules__xfmpl_appfrmext') }}
where svrejectflag = TRUE and svLoadApptPdct = 'Y'
)

SELECT
  PL_APP_ID,
  NOMINATED_BRANCH_ID,
  PL_PACKAGE_CAT_ID,
  ETL_D,
  ORIG_ETL_D,
  EROR_C
from tgtapptpdctrejectsds