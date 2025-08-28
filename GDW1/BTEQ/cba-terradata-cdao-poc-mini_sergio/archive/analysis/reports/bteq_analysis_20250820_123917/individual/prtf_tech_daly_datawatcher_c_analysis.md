# prtf_tech_daly_datawatcher_c.sql - BTEQ Analysis

## File Overview
- **File Name**: prtf_tech_daly_datawatcher_c.sql
- **Analysis Status**: ✅ Success
- **Control Statements**: 8
- **SQL Blocks**: 1

## Control Flow Analysis

| Line | Type | Statement |
|------|------|-----------|
| 1 | RUN | `.RUN FILE=%%BTEQ_LOGON_SCRIPT%%` |
| 2 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 79 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 83 | LOGOFF | `.LOGOFF` |
| 85 | LABEL | `.LABEL EXITERR` |
| 87 | LOGOFF | `.LOGOFF` |
| 89 | LABEL | `.LABEL REPOLL` |
| 91 | LOGOFF | `.LOGOFF` |

## SQL Blocks Analysis

### SQL Block 1 (Lines 25-78)
- **Complexity Score**: 143
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 2

#### 📝 Original Teradata SQL:
```sql
SELECT SRCE_FND
FROM
(
--Check Analytics at table level
 SELECT 1 AS SRCE_FND
 FROM (
       SELECT 
         UPI.TRGT_M AS TRGT_M
        ,UPI.SRCE_M as SRCE_M
       FROM %%VTECH%%.UTIL_PROS_ISAC AS UPI

-- Want the data loaded for yesterday

       WHERE UPI.BTCH_RUN_D = CURRENT_DATE - 1
       AND UPI.TRGT_M IN (
--Get the list of pre-requisite tables 
                            SELECT UP1.PARM_LTRL_STRG_X
                            FROM %%VTECH%%.UTIL_PARM UP1
                            WHERE UP1.PARM_M = 'PRTF_TECH_CHNG_SCHE_DEPN_TABL'
                           )
--Get the list of pre-requisite sources
       AND UPI.SRCE_M LIKE ANY
          (
           SELECT '%'||TRIM(UP1.PARM_LTRL_STRG_X)||'%' AS SRCE_M
           FROM %%VTECH%%.UTIL_PARM UP1
           WHERE UP1.PARM_M = 'PRTF_TECH_CHNG_SCHE_DEPN_SRCE'
          )
--and check they have been loaded
       AND UPI.COMT_F = 'Y'
       --AND UPI.SRCE_SYST_M = 'SAP'

       QUALIFY RANK() OVER (PARTITION BY UPI.TRGT_M
                                ORDER BY UPI.BTCH_RUN_D DESC) = 1
      ) DT (TRGT_M, SRCE_M)

--and check all relevant sources have loaded
 HAVING COUNT(TRGT_M) =
        (
         SELECT UP2.PARM_LTRL_N
         FROM %%VTECH%%.UTIL_PARM UP2
         WHERE UP2.PARM_M = 'PRTF_TECH_CHNG_SCHE_DEPN_SRCE_LOAD'
        )
       
) SRCES
GROUP BY 1
HAVING COUNT(SRCE_FND) = 1    
;


-- This info is for CBM use only
-- $LastChangedBy: jelifft $
-- $LastChangedDate: 2013-07-04 09:44:07 +1000 (Thu, 04 Jul 2013) $
-- $LastChangedRevision: 12230 $
```

#### ❄️ Converted Snowflake SQL:
```sql
SELECT SRCE_FND FROM (SELECT 1 AS SRCE_FND FROM (SELECT UPI.TRGT_M AS TRGT_M, UPI.SRCE_M AS SRCE_M FROM %%VTECH%%.UTIL_PROS_ISAC AS UPI WHERE UPI.BTCH_RUN_D = CURRENT_DATE - 1 AND UPI.TRGT_M IN (SELECT UP1.PARM_LTRL_STRG_X FROM %%VTECH%%.UTIL_PARM AS UP1 WHERE UP1.PARM_M = 'PRTF_TECH_CHNG_SCHE_DEPN_TABL') AND UPI.SRCE_M LIKE ANY (SELECT '%' || TRIM(UP1.PARM_LTRL_STRG_X) || '%' AS SRCE_M FROM %%VTECH%%.UTIL_PARM AS UP1 WHERE UP1.PARM_M = 'PRTF_TECH_CHNG_SCHE_DEPN_SRCE') AND UPI.COMT_F = 'Y' QUALIFY RANK() OVER (PARTITION BY UPI.TRGT_M ORDER BY UPI.BTCH_RUN_D DESC NULLS LAST) = 1) AS DT(TRGT_M, SRCE_M) HAVING COUNT(TRGT_M) = (SELECT UP2.PARM_LTRL_N FROM %%VTECH%%.UTIL_PARM AS UP2 WHERE UP2.PARM_M = 'PRTF_TECH_CHNG_SCHE_DEPN_SRCE_LOAD')) AS SRCES GROUP BY 1 HAVING COUNT(SRCE_FND) = 1
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- QUALIFY clause
- Variable substitution

#### ⚡ Optimized SQL:
```sql
WITH "_u_0" AS (
  SELECT
    "up1"."parm_ltrl_strg_x" AS "parm_ltrl_strg_x"
  FROM "%%vtech%%"."util_parm" AS "up1"
  WHERE
    "up1"."parm_m" = 'PRTF_TECH_CHNG_SCHE_DEPN_TABL'
  GROUP BY
    "up1"."parm_ltrl_strg_x"
), "dt" AS (
  SELECT
    "upi"."trgt_m" AS "trgt_m"
  FROM "%%vtech%%"."util_pros_isac" AS "upi"
  LEFT JOIN "_u_0" AS "_u_0"
    ON "_u_0"."parm_ltrl_strg_x" = "upi"."trgt_m"
  WHERE
    "upi"."btch_run_d" = CURRENT_DATE - 1
    AND "upi"."comt_f" = 'Y'
    AND "upi"."srce_m" LIKE ANY (
      SELECT
        '%' || TRIM("up1"."parm_ltrl_strg_x") || '%' AS "srce_m"
      FROM "%%vtech%%"."util_parm" AS "up1"
      WHERE
        "up1"."parm_m" = 'PRTF_TECH_CHNG_SCHE_DEPN_SRCE'
    )
    AND NOT "_u_0"."parm_ltrl_strg_x" IS NULL
  QUALIFY
    RANK() OVER (PARTITION BY "upi"."trgt_m" ORDER BY "upi"."btch_run_d" DESC NULLS LAST) = 1
), "srces" AS (
  SELECT
    1 AS "srce_fnd"
  FROM "dt" AS "dt"
  JOIN "%%vtech%%"."util_parm" AS "up2"
    ON "up2"."parm_m" = 'PRTF_TECH_CHNG_SCHE_DEPN_SRCE_LOAD'
  HAVING
    COUNT("dt"."trgt_m") = MAX("up2"."parm_ltrl_n")
)
SELECT
  "srces"."srce_fnd" AS "srce_fnd"
FROM "srces" AS "srces"
GROUP BY
  "srces"."srce_fnd"
HAVING
  COUNT("srces"."srce_fnd") = 1
```

#### 📊 SQL Metadata:
- **Tables**: UTIL_PARM, UTIL_PROS_ISAC
- **Columns**: BTCH_RUN_D, COMT_F, PARM_LTRL_N, PARM_LTRL_STRG_X, PARM_M, SRCE_FND, SRCE_M, TRGT_M
- **Functions**: None, RANK, SRCE_FND, TRGT_M, UP1.PARM_LTRL_STRG_X, UPI.BTCH_RUN_D = CURRENT_DATE - 1, UPI.BTCH_RUN_D = CURRENT_DATE - 1 AND UPI.TRGT_M IN (SELECT UP1.PARM_LTRL_STRG_X FROM %%VTECH%%.UTIL_PARM AS UP1 WHERE UP1.PARM_M = 'PRTF_TECH_CHNG_SCHE_DEPN_TABL'), UPI.BTCH_RUN_D = CURRENT_DATE - 1 AND UPI.TRGT_M IN (SELECT UP1.PARM_LTRL_STRG_X FROM %%VTECH%%.UTIL_PARM AS UP1 WHERE UP1.PARM_M = 'PRTF_TECH_CHNG_SCHE_DEPN_TABL') AND UPI.SRCE_M LIKE ANY (SELECT '%' || TRIM(UP1.PARM_LTRL_STRG_X) || '%' AS SRCE_M FROM %%VTECH%%.UTIL_PARM AS UP1 WHERE UP1.PARM_M = 'PRTF_TECH_CHNG_SCHE_DEPN_SRCE')
- **Window_Functions**: RANK

## Migration Recommendations

### Suggested Migration Strategy
**Medium complexity** - Incremental model with DCF hooks recommended

### DCF Mapping
- **Error Handling**: Convert `.IF ERRORCODE` patterns to DCF error handling macros
- **File Operations**: Replace IMPORT/EXPORT with dbt seeds or external tables
- **Statistics**: Replace COLLECT STATS with Snowflake post-hooks
- **Stored Procedures**: Map CALL statements to DCF stored procedure macros

---

*Generated by BTEQ Parser Service - 2025-08-20 12:39:25*
