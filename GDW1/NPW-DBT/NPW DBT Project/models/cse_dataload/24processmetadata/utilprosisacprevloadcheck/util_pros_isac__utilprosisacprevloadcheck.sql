WITH util_pros_isac AS
(
SELECT
  COUNT(*) - 1 AS NUM_LOAD_ERR
FROM
  {{ cvar('mart_db') }}.{{ cvar('gdw_acct_db') }}."util_pros_isac"
WHERE
  "srce_syst_m" = '{{ cvar("app_release") }}'
  AND "btch_run_d" = TO_DATE('{{ cvar("etl_process_dt") }}' , 'YYYYMMDD') - 1
  AND (
    ("comt_f" = 'N' OR "comt_f" IS NULL)
    OR ("succ_f" = 'N' OR "succ_f" IS NULL)
  )
)

SELECT NUM_LOAD_ERR
FROM util_pros_isac