{{
    config(
        pre_hook=[
        'CREATE OR REPLACE TRANSIENT TABLE ' ~ cvar("intermediate_db") ~ '.' ~cvar("datasets_schema") ~ '.' ~ cvar("base_dir") ~ '__dataset__'~ cvar("tgt_table") ~ '_I_'~ cvar("run_stream") ~ '_'~ cvar("etl_process_dt") ~ '__DS (APPT_PDCT_I VARCHAR(255) NOT NULL, DEBT_ABN_X VARCHAR(255), DEBT_BUSN_M VARCHAR(40), SMPL_APPT_F VARCHAR(1), JOB_COMM_CATG_C VARCHAR(4), APPT_QLFY_C VARCHAR(2), ACQR_TYPE_C VARCHAR(4), ACQR_ADHC_X VARCHAR(255), ACQR_SRCE_C VARCHAR(4), PDCT_N NUMBER(38,0) NOT NULL, APPT_I VARCHAR(255) NOT NULL, SRCE_SYST_C VARCHAR(3) NOT NULL, SRCE_SYST_APPT_PDCT_I VARCHAR(255) NOT NULL, LOAN_FNDD_METH_C VARCHAR(4), NEW_ACCT_F VARCHAR(1), BROK_PATY_I VARCHAR(255), COPY_FROM_OTHR_APPT_F VARCHAR(1), EFFT_D DATE NOT NULL, EXPY_D DATE NOT NULL, PROS_KEY_EFFT_I NUMBER(10,0) NOT NULL, PROS_KEY_EXPY_I NUMBER(10,0), EROR_SEQN_I NUMBER(10,0), APPT_PDCT_CATG_C VARCHAR(4), APPT_PDCT_DURT_C VARCHAR(4), ASES_D DATE);',
        'CREATE OR REPLACE TRANSIENT TABLE ' ~ cvar("intermediate_db") ~ '.' ~ cvar("datasets_schema") ~ '.' ~ cvar("base_dir") ~ '__dataset__'~ cvar("tgt_table") ~ '_U_'~ cvar("run_stream") ~ '_'~ cvar("etl_process_dt") ~ '__DS (APPT_PDCT_I VARCHAR(255) NOT NULL, APPT_I VARCHAR(255) NOT NULL, EFFT_D DATE NOT NULL, EXPY_D DATE NOT NULL, PROS_KEY_EXPY_I NUMBER(10,0));'
        ]
    )
}}

with cte as(
    select 1 as dummy
)

SELECT
    dummy
FROM cte
