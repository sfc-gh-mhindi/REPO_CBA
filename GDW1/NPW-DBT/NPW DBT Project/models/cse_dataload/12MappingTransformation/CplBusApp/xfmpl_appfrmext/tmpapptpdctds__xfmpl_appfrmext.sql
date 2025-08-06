{{
  config(
    post_hook=[
      "INSERT overwrite INTO " ~ cvar("datasets_schema") ~ "."~ cvar("base_dir") ~"__dataset__Tmp_"~ cvar("run_stream") ~ "_APPT_PDCT__DS SELECT * FROM {{ this }}"
    ]
  )
}}

with xfmbusinessrules as (
select
    'CSE' || 'PP' || PL_APP_ID as APPT_PDCT_I,
    'PP' as APPT_QLFY_C,
    null as ACQR_TYPE_C,
    null as ACQR_ADHC_X,
    null as ACQR_SRCE_C,
    PDCT_N,
    'CSE' || 'PL' || PL_APP_ID as APPT_I,
    'CSE' as SRCE_SYST_C,
    PL_PACKAGE_CAT_ID as SRCE_SYST_APPT_PDCT_I,
    null as LOAN_FNDD_METH_C,
    'N' as NEW_ACCT_F,
    null as BROK_PATY_I,
    'N' as COPY_FROM_OTHR_APPT_F,
    to_date('{{ cvar("etl_process_dt") }}', 'YYYYMMDD') as EFFT_D,
    to_date('99991231', 'YYYYMMDD') as EXPY_D,
    0 as PROS_KEY_EFFT_I,
    null as PROS_KEY_EXPY_I,
    null as EROR_SEQN_I,
    '{{ cvar("run_stream") }}' as RUN_STRM,
    null as APPT_PDCT_CATG_C,
    null as ASES_D,
    null as APPT_PDCT_DURT_C
from {{ ref('xfmbusinessrules__xfmpl_appfrmext') }}
where svLoadApptPdct = 'Y'
)

select 
	APPT_PDCT_I,
	APPT_QLFY_C,
	ACQR_TYPE_C,
	ACQR_ADHC_X,
	ACQR_SRCE_C,
	PDCT_N,
	APPT_I,
	SRCE_SYST_C,
	SRCE_SYST_APPT_PDCT_I,
	LOAN_FNDD_METH_C,
	NEW_ACCT_F,
	BROK_PATY_I,
	COPY_FROM_OTHR_APPT_F,
	EFFT_D,
	EXPY_D,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I,
	EROR_SEQN_I,
	RUN_STRM,
	APPT_PDCT_CATG_C,
	ASES_D,
	APPT_PDCT_DURT_C
from xfmbusinessrules