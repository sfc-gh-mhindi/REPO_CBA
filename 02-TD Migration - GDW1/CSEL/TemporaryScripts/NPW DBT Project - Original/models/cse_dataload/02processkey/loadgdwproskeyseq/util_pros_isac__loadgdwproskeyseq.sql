with 
before_cte as(
    SELECT 
        dummy
    FROM {{ ref('before__loadgdwproskeyseq') }}
    where 1=2
),
util_pros_isac as
(
SELECT
    TRIM(srce_m) || '_' || TRIM(trgt_m) as CONV_M,
    pros_key_i as PROS_KEY_I
FROM
    {{ cvar('stg_ctl_db') }}.{{ cvar("gdw_proc_acct_db") }}.util_pros_isac
WHERE
    srce_syst_m = '{{ cvar("app_release") }}'
    AND btch_run_d = TO_DATE('{{ cvar("etl_process_dt") }}' , 'YYYYMMDD')
)

select * from util_pros_isac