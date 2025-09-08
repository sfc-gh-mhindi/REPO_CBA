{{
    config(
       pre_hook = [
    'CREATE OR REPLACE TRANSIENT TABLE ' ~ cvar("intermediate_db") ~ '.' ~ cvar("datasets_schema") ~ '.' ~ cvar("base_dir") ~ '__dataset__APPT_DEPT_I_' ~ cvar("run_stream") ~ '_' ~ cvar("etl_process_dt") ~ '__DS (APPT_I VARCHAR(255) NOT NULL, DEPT_ROLE_C VARCHAR(5) NOT NULL, EFFT_D DATE NOT NULL, DEPT_I VARCHAR(255), EXPY_D DATE NOT NULL, PROS_KEY_EFFT_I NUMBER(10,0) NOT NULL, PROS_KEY_EXPY_I NUMBER(10,0), EROR_SEQN_I NUMBER(10,0));',
    'CREATE OR REPLACE TRANSIENT TABLE ' ~ cvar("intermediate_db") ~ '.' ~ cvar("datasets_schema") ~ '.' ~ cvar("base_dir") ~ '__dataset__' ~ cvar("tgt_table") ~ '_U_' ~ cvar("run_stream") ~ '_' ~ cvar("etl_process_dt") ~ '__DS (DEPT_I VARCHAR(255), APPT_I VARCHAR(255) NOT NULL, DEPT_ROLE_C VARCHAR(5) NOT NULL, EFFT_D DATE NOT NULL, EXPY_D DATE NOT NULL, PROS_KEY_EXPY_I NUMBER(10,0));',
    'CREATE OR REPLACE TRANSIENT TABLE ' ~ cvar("intermediate_db") ~ '.' ~ cvar("datasets_schema") ~ '.' ~ cvar("base_dir") ~ '__dataset__DEPT_APPT_I_' ~ cvar("run_stream") ~ '_' ~ cvar("etl_process_dt") ~ '__DS (APPT_I VARCHAR(255) NOT NULL, DEPT_ROLE_C VARCHAR(5) NOT NULL, EFFT_D DATE NOT NULL, DEPT_I VARCHAR(255) NOT NULL, EXPY_D DATE NOT NULL, PROS_KEY_EFFT_I NUMBER(10,0) NOT NULL, PROS_KEY_EXPY_I NUMBER(10,0), EROR_SEQN_I NUMBER(10,0));',   
    'CREATE OR REPLACE TRANSIENT TABLE ' ~ cvar("intermediate_db") ~ '.' ~ cvar("datasets_schema") ~ '.' ~ cvar("base_dir") ~ '__dataset__DEPT_APPT_U_' ~ cvar("run_stream") ~ '_' ~ cvar("etl_process_dt") ~ '__DS (DEPT_I VARCHAR(255) NOT NULL, APPT_I VARCHAR(255) NOT NULL, DEPT_ROLE_C VARCHAR(5) NOT NULL, EFFT_D DATE NOT NULL, EXPY_D DATE NOT NULL, PROS_KEY_EXPY_I NUMBER(10,0));'
]
    )
}}


WITH cte as(
Select 1 as dummy
)

SELECT
dummy
FROM cte