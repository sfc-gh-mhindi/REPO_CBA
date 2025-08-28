## BTEQ → DCF Agentic Conversion: Design Options (Focused on `current_state/bteq_sql` and `gdw2_xfm_frmw`)

### Objective
Convert Teradata BTEQ scripts to simple, maintainable DBT models using the DCF macro framework in `gdw2_xfm_frmw`. The main SQL becomes a DBT model; control-flow, orchestration, and logging shift to DBT pre/post hooks.

### Observed BTEQ Patterns (from `current_state/bteq_sql`)
- **Simple INSERT-SELECT**: e.g., `ACCT_BALN_BKDT_ISRT.sql` inserts from staging to target.
- **Multi-step transforms with effective-dating and overlaps**: e.g., `DERV_ACCT_PATY_04_POP_CURR_TABL.sql` with `.IMPORT` for `EXTR_D`, multiple DELETE/INSERT blocks, windowing, and UNION ALL.
- **Stored procedure orchestration**: e.g., `sp_get_pros_key.sql` calling `SP_GET_PROS_KEY` using files (`.IMPORT/.EXPORT`, `.OS rm`).
- **Statistics and housekeeping**: `COLLECT STATS`, file IO, labels and error traps (`.IF ERRORCODE`, `.GOTO EXITERR`).

### Relevant DBT/DCF Capabilities (from `gdw2_xfm_frmw`)
- Materializations: `incremental_dcf_ibrg.sql`, `insert_append_dcf_ibrg.sql`, `full_apply_dcf_ibrg.sql`.
- Control/Logging: `macros/dcf/process_control.sql`, `macros/dcf/logging.sql`.
- Project vars for database/schema routing: `dbt_project.yml` (`intermediate_database`, `dcf_database`, `target_database`, etc.).

---

### Design Option 1: RAG-Enhanced Single-Agent Converter (Recommended MVP)

Agent performs: parse → classify → retrieve examples → map → generate → validate.

- **Parsing & Classification**
  - Identify main SQL (INSERT/SELECT/MERGE) and side-effects (`.IMPORT/.EXPORT`, `.OS`, `.IF ERRORCODE`).
  - Classify into patterns: simple-insert, insert-append, delta-upsert, full-apply, orchestration-only.

- **Mapping Rules (concise)**
  - Simple INSERT-SELECT → DBT model with `materialized='insert_append_dcf_ibrg'` (or `table` if one-off load).
  - Multi-step effective-dating → consolidate into a single SELECT with CTEs; use `full_apply_dcf` (type-2 close/open) or `incremental_dcf_ibrg` if delta-driven.
  - Stored-proc driven process keys → replace with DCF `register_process_instance`, `check_dcf_dependencies`, and use `BUS_DT` via DCF control tables (no filesystem IO).
  - `.IF ERRORCODE`/labels → pre/post hooks with `validate_*` and `update_process_status` macros.

- **Context Pack (RAG)**
  - Vector store of BTEQ→DBT exemplars (small pairs for each pattern) + macro usage cheatsheet.
  - Project vars snapshot and allowed macro list to constrain generation.

- **Generation Contract**
  - Output only: `models/.../*.sql` with a single config block, one SELECT/CTE tree, `pre_hook` and `post_hook` arrays.
  - No external files; no temporary tables unless via model SQL.

- **Example: ACCT_BALN_BKDT_ISRT (simple insert-select)**
```sql
{{ config(
  materialized='insert_append_dcf_ibrg',
  database=var('intermediate_database'),
  schema=var('intermediate_schema'),
  tags=['acct_baln_bkdt','insert_append'],
  pre_hook=[
    "{{ check_dcf_dependencies('BV_PDS_TRAN') }}",
    "{{ register_process_instance(this.name, 'BV_PDS_TRAN') }}"
  ],
  post_hook=[
    "{{ log_dcf_exec_msg(process_name=this.name, stream_name='BV_PDS_TRAN', message_type=10, message_text='Completed') }}",
    "{{ update_process_status(this.name, 'BV_PDS_TRAN', 'COMPLETED') }}"
  ]
) }}

select
  ACCT_I,
  BALN_TYPE_C,
  CALC_FUNC_C,
  TIME_PERD_C,
  BALN_A,
  CALC_F,
  SRCE_SYST_C,
  ORIG_SRCE_SYST_C,
  LOAD_D,
  BKDT_EFFT_D,
  BKDT_EXPY_D,
  PROS_KEY_EFFT_I,
  PROS_KEY_EXPY_I,
  BKDT_PROS_KEY_I
from {{ source('staging','ACCT_BALN_BKDT_STG2') }}
```

- **Example: DERV_ACCT_PATY_04_POP_CURR_TABL (multi-step, use business date)**
```sql
{{ config(
  materialized='full_apply_dcf',
  database=var('intermediate_database'),
  schema=var('intermediate_schema'),
  tags=['derv_acct_paty','full_apply'],
  unique_key=['ACCT_I','PATY_I','PATY_ACCT_REL_C'],
  pre_hook=[
    "{{ check_dcf_dependencies('BV_PDS_TRAN') }}",
    "{{ register_process_instance(this.name, 'BV_PDS_TRAN') }}"
  ],
  post_hook=[
    "{{ log_dcf_exec_msg(process_name=this.name, stream_name='BV_PDS_TRAN', message_type=10, message_text='Completed') }}",
    "{{ update_process_status(this.name, 'BV_PDS_TRAN', 'COMPLETED') }}"
  ]
) }}

with busdate as (
  select BUSINESS_DATE as EXTR_D
  from {{ var('dcf_database') }}.DCF_T_STRM_BUS_DT
  where STRM_NAME = 'BV_PDS_TRAN'
  qualify row_number() over (order by BUSINESS_DATE desc) = 1
),
acct_paty_dedup as (
  select AP.ACCT_I, AP.PATY_I, AP.ACCT_I as ASSC_ACCT_I, AP.PATY_ACCT_REL_C,
         'N' as PRFR_PATY_F, AP.SRCE_SYST_C, AP.EFFT_D,
         case when EFFT_D = EXPY_D then EXPY_D
              when EXPY_D >= b.EXTR_D then to_date('9999-12-31')
              else EXPY_D end as EXPY_D,
         AP.ROW_SECU_ACCS_C
  from {{ source('bus','ACCT_PATY') }} AP
  cross join busdate b
  where b.EXTR_D between AP.EFFT_D and AP.EXPY_D
  qualify row_number() over (partition by ACCT_I, PATY_I, PATY_ACCT_REL_C order by EFFT_D) = 1
),
derv_acct_paty_curr as (
  select ACCT_I, PATY_I, ASSC_ACCT_I, PATY_ACCT_REL_C, PRFR_PATY_F, SRCE_SYST_C,
         EFFT_D, EXPY_D, ROW_SECU_ACCS_C
  from acct_paty_dedup
  union all
  /* additional unions translating BPS/LMS/MID/MTX logic using intersections */
  select ...
)
select * from derv_acct_paty_curr
```

Notes:
- `.IMPORT/.EXPORT` file usage replaced by reading `BUSINESS_DATE` from DCF control tables.
- `COLLECT STATS` omitted; Snowflake optimizer handles stats.
- `.IF ERRORCODE` flows replaced by pre/post-hook validations and status updates.

---

### Design Option 2: Multi-Agent Pipeline

- **Parser Agent**: Extract SQL, variables (`%%ENV_C%%`, `%%STRM_C%%`), control commands.
- **Analyzer Agent**: Classify pattern, detect effective-dating, windowing, delta behavior.
- **Mapper Agent**: Choose materialization (`insert_append_dcf_ibrg`, `full_apply_dcf`, `incremental_dcf_ibrg`), map `.IMPORT` to DCF bus date access, replace SP calls with DCF macros.
- **Generator Agent**: Emit DBT model + pre/post hooks only; no external side effects.
- **Validator Agent**: Lint SQL, ensure macros exist, ensure no temp tables, ensure only approved vars/macros are used.

This is heavier but extensible for edge cases and auto-fix loops.

---

### Prompt and Context Engineering

- **System Prompt (single-agent)**
```text
You convert Teradata BTEQ to DBT models using DCF macros in gdw2_xfm_frmw. Keep DBT simple:
- main SQL only; orchestration via pre/post hooks
- choose one of: insert_append_dcf_ibrg, full_apply_dcf, incremental_dcf_ibrg
- use DCF control tables for business date; do not use files or OS commands
- no temp tables unless within a CTE tree
```

- **Few-shot Exemplar Keys**
  - BTEQ INSERT→ DBT `insert_append_dcf_ibrg`
  - Multi-step effective-dating→ DBT `full_apply_dcf` with CTEs
  - SP process key→ replace with `register_process_instance` and `log_dcf_exec_msg`

- **Context Pack**
  - Paths: `macros/dcf/*.sql`, `macros/*dcf_ibrg.sql`, `dbt_project.yml` vars
  - Source naming contract: map `%%DDSTG%%`→ `source('staging', ...)`, `%%VTECH%%`→ `source('bus', ...)` (adjust per actual source.yml)
  - Disallow: `.OS`, file IO, `COLLECT STATS`, labels

---

### Guardrails & Conventions
- Always include `pre_hook` with `check_dcf_dependencies` + `register_process_instance`.
- Always include `post_hook` with `log_dcf_exec_msg` + `update_process_status`.
- Prefer CTEs over intermediate DELETE/INSERT steps; one final SELECT.
- Use `var('intermediate_database')` and `var('intermediate_schema')` for DBT model routing.
- Choose materialization:
  - Append-only loads → `insert_append_dcf_ibrg`
  - SCD2/full image replacement → `full_apply_dcf`
  - Delta merge semantics → `incremental_dcf_ibrg`

---

### Implementation Steps (MVP)
1. Build exemplar library for the three patterns using 4–6 scripts from `bteq_sql`.
2. Implement single-agent with RAG over exemplars + macro docs.
3. Emit DBT models into `gdw2_xfm_frmw/models/...` with hooks only.
4. Add unit checks: macro existence, prohibited constructs, source refs.

---

### Appendix: Quick Mapping Table

| BTEQ Feature | DBT/DCF Mapping |
|---|---|
| `.RUN`, `.LOGON` | None (DBT profile handles connections) |
| `.IF ERRORCODE`, `.GOTO` | Pre/Post hooks with `validate_*`, `update_process_status` |
| `.IMPORT/.EXPORT`, `.OS rm` | Read `BUSINESS_DATE` from DCF tables; avoid filesystem IO |
| `COLLECT STATS` | Omit (Snowflake-managed) |
| `CALL SP_GET_PROS_KEY(...)` | Use `register_process_instance`, `log_dcf_exec_msg`; optionally wrap stored proc as a Snowflake procedure and call in pre_hook if strictly required |


