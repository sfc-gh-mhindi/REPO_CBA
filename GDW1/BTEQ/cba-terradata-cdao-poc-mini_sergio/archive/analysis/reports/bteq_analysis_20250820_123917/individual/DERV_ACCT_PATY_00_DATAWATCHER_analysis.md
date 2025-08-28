# DERV_ACCT_PATY_00_DATAWATCHER.sql - BTEQ Analysis

## File Overview
- **File Name**: DERV_ACCT_PATY_00_DATAWATCHER.sql
- **Analysis Status**: ✅ Success
- **Control Statements**: 12
- **SQL Blocks**: 2

## Control Flow Analysis

| Line | Type | Statement |
|------|------|-----------|
| 1 | RUN | `.run file=%%BTEQ_LOGON_SCRIPT%%` |
| 44 | OS_CMD | `.OS rm /cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_extr_date.txt` |
| 51 | EXPORT | `.EXPORT DATA FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_extr_date.txt` |
| 63 | EXPORT | `.EXPORT RESET` |
| 66 | IF_ERRORCODE | `.IF ERRORCODE <> 0 THEN .GOTO EXITERR` |
| 75 | IMPORT | `.IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_extr_date.txt` |
| 158 | IF_ERRORCODE | `.IF ERRORCODE <> 0 THEN .GOTO EXITERR` |
| 163 | LOGOFF | `.LOGOFF` |
| 165 | LABEL | `.LABEL EXITERR` |
| 167 | LOGOFF | `.LOGOFF` |
| 169 | LABEL | `.LABEL REPOLL` |
| 171 | LOGOFF | `.LOGOFF` |

## SQL Blocks Analysis

### SQL Block 1 (Lines 54-62)
- **Complexity Score**: 39
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 2

#### 📝 Original Teradata SQL:
```sql
SELECT CAST(BTCH_RUN_D + 1 AS CHAR(10))
     FROM %%VTECH%%.UTIL_BTCH_ISAC UBI

      WHERE UBI.BTCH_STUS_C = 'COMT'
        AND UBI.SRCE_SYST_M = '%%SRCE_SYST_M%%'                    
       
    QUALIFY RANK() OVER (ORDER BY BTCH_RUN_D  DESC,STUS_CHNG_S DESC) = 1
  ;
```

#### ❄️ Converted Snowflake SQL:
```sql
SELECT CAST(BTCH_RUN_D + 1 AS CHAR(10)) FROM %%VTECH%%.UTIL_BTCH_ISAC AS UBI WHERE UBI.BTCH_STUS_C = 'COMT' AND UBI.SRCE_SYST_M = '%%SRCE_SYST_M%%' QUALIFY RANK() OVER (ORDER BY BTCH_RUN_D DESC NULLS LAST, STUS_CHNG_S DESC NULLS LAST) = 1
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- QUALIFY clause
- Variable substitution

#### ⚡ Optimized SQL:
```sql
SELECT
  CAST("ubi"."btch_run_d" + 1 AS CHAR(10)) AS "_col_0"
FROM "%%vtech%%"."util_btch_isac" AS "ubi"
WHERE
  "ubi"."btch_stus_c" = 'COMT' AND "ubi"."srce_syst_m" = '%%SRCE_SYST_M%%'
QUALIFY
  RANK() OVER (ORDER BY "ubi"."btch_run_d" DESC NULLS LAST, "ubi"."stus_chng_s" DESC NULLS LAST) = 1
```

#### 📊 SQL Metadata:
- **Tables**: UTIL_BTCH_ISAC
- **Columns**: BTCH_RUN_D, BTCH_STUS_C, SRCE_SYST_M, STUS_CHNG_S
- **Functions**: BTCH_RUN_D + 1, RANK, UBI.BTCH_STUS_C = 'COMT'
- **Window_Functions**: RANK

### SQL Block 2 (Lines 78-157)
- **Complexity Score**: 218
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 2

#### 📝 Original Teradata SQL:
```sql
SELECT SRCE_FND
FROM
(
-- Check SAP BP loads at table level

 SELECT 1 AS SRCE_FND
 FROM (
       SELECT UPI.TRGT_M AS TRGT_M
             ,UPI.SRCE_SYST_M AS SRCE_M
         FROM  %%VPATY%%.UTIL_PROS_ISAC UPI
        WHERE BTCH_RUN_D IS NOT NULL
          AND BTCH_RUN_D <> '0001-01-01'

-- Want the data loaded for the current extract date
       
          AND UPI.BTCH_RUN_D  = :EXTR_D
          AND UPI.TRGT_M IN (
      
--Get the list of pre-requisite tables
 
                            SELECT UP1.PARM_LTRL_STRG_X
                            FROM %%VTECH%%.UTIL_PARM UP1
                            WHERE UP1.PARM_M = 'DERV_ACCT_PATY_SAPBP_SCHE_DEPN_TABL'
                           )
                           
--Get the list of pre-requisite sources

         AND UPI.SRCE_SYST_M IN
          (
           SELECT UP1.PARM_LTRL_STRG_X AS SRCE_SYST_M
           FROM %%VTECH%%.UTIL_PARM UP1
           WHERE UP1.PARM_M = 'DERV_ACCT_PATY_SAPBP_SCHE_DEPN_SRCE'
          )
          
--and check they have been loaded

      AND UPI.COMT_F = 'Y'
     

       QUALIFY RANK() OVER (PARTITION BY UPI.TRGT_M
                                ORDER BY UPI.BTCH_RUN_D DESC) = 1
      ) DT (TRGT_M, SRCE_M)

--and check all relevant sources have loaded

 HAVING COUNT(TRGT_M) =
        (
         SELECT UP2.PARM_LTRL_N
         FROM %%VTECH%%.UTIL_PARM UP2
         WHERE UP2.PARM_M = 'DERV_ACCT_PATY_SAPBP_SCHE_DEPN_SRCE_LOAD'
        )
 
 UNION ALL

-- Check portfolio persisted tables loads at stream level

 SELECT 1 AS SRCE_FND
 FROM (
       SELECT UBI.SRCE_SYST_M 
       FROM %%VTECH%%.UTIL_BTCH_ISAC UBI

      WHERE UBI.BTCH_STUS_C = 'COMT'
        AND UBI.SRCE_SYST_M = 'PRTF_TECH'                    
        AND UBI.BTCH_RUN_D  >= :EXTR_D
    QUALIFY RANK() OVER (ORDER BY BTCH_RUN_D  DESC,STUS_CHNG_S DESC) = 1
    
     ) DT2 (SRCE_SYST_M)
 HAVING COUNT(SRCE_SYST_M) = 1
 
) SRCES
GROUP BY 1
HAVING COUNT(SRCE_FND) = (
         SELECT UP2.PARM_LTRL_N
         FROM %%VTECH%%.UTIL_PARM UP2
         WHERE UP2.PARM_M = 'DERV_ACCT_PATY_SCHE_DEPN_SRCE_LOAD'
        )    
      
  
;
```

#### ❄️ Converted Snowflake SQL:
```sql
SELECT SRCE_FND FROM (SELECT 1 AS SRCE_FND FROM (SELECT UPI.TRGT_M AS TRGT_M, UPI.SRCE_SYST_M AS SRCE_M FROM %%VPATY%%.UTIL_PROS_ISAC AS UPI WHERE NOT BTCH_RUN_D IS NULL AND BTCH_RUN_D <> '0001-01-01' AND UPI.BTCH_RUN_D = :EXTR_D AND UPI.TRGT_M IN (SELECT UP1.PARM_LTRL_STRG_X FROM %%VTECH%%.UTIL_PARM AS UP1 WHERE UP1.PARM_M = 'DERV_ACCT_PATY_SAPBP_SCHE_DEPN_TABL') AND UPI.SRCE_SYST_M IN (SELECT UP1.PARM_LTRL_STRG_X AS SRCE_SYST_M FROM %%VTECH%%.UTIL_PARM AS UP1 WHERE UP1.PARM_M = 'DERV_ACCT_PATY_SAPBP_SCHE_DEPN_SRCE') AND UPI.COMT_F = 'Y' QUALIFY RANK() OVER (PARTITION BY UPI.TRGT_M ORDER BY UPI.BTCH_RUN_D DESC NULLS LAST) = 1) AS DT(TRGT_M, SRCE_M) HAVING COUNT(TRGT_M) = (SELECT UP2.PARM_LTRL_N FROM %%VTECH%%.UTIL_PARM AS UP2 WHERE UP2.PARM_M = 'DERV_ACCT_PATY_SAPBP_SCHE_DEPN_SRCE_LOAD') UNION ALL SELECT 1 AS SRCE_FND FROM (SELECT UBI.SRCE_SYST_M FROM %%VTECH%%.UTIL_BTCH_ISAC AS UBI WHERE UBI.BTCH_STUS_C = 'COMT' AND UBI.SRCE_SYST_M = 'PRTF_TECH' AND UBI.BTCH_RUN_D >= :EXTR_D QUALIFY RANK() OVER (ORDER BY BTCH_RUN_D DESC NULLS LAST, STUS_CHNG_S DESC NULLS LAST) = 1) AS DT2(SRCE_SYST_M) HAVING COUNT(SRCE_SYST_M) = 1) AS SRCES GROUP BY 1 HAVING COUNT(SRCE_FND) = (SELECT UP2.PARM_LTRL_N FROM %%VTECH%%.UTIL_PARM AS UP2 WHERE UP2.PARM_M = 'DERV_ACCT_PATY_SCHE_DEPN_SRCE_LOAD')
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
    "up1"."parm_m" = 'DERV_ACCT_PATY_SAPBP_SCHE_DEPN_TABL'
  GROUP BY
    "up1"."parm_ltrl_strg_x"
), "_u_1" AS (
  SELECT
    "up1"."parm_ltrl_strg_x" AS "srce_syst_m"
  FROM "%%vtech%%"."util_parm" AS "up1"
  WHERE
    "up1"."parm_m" = 'DERV_ACCT_PATY_SAPBP_SCHE_DEPN_SRCE'
  GROUP BY
    "up1"."parm_ltrl_strg_x"
), "dt" AS (
  SELECT
    "upi"."trgt_m" AS "trgt_m"
  FROM "%%vpaty%%"."util_pros_isac" AS "upi"
  LEFT JOIN "_u_0" AS "_u_0"
    ON "_u_0"."parm_ltrl_strg_x" = "upi"."trgt_m"
  LEFT JOIN "_u_1" AS "_u_1"
    ON "_u_1"."srce_syst_m" = "upi"."srce_syst_m"
  WHERE
    "upi"."btch_run_d" <> '0001-01-01'
    AND "upi"."btch_run_d" = :EXTR_D
    AND "upi"."comt_f" = 'Y'
    AND NOT "_u_0"."parm_ltrl_strg_x" IS NULL
    AND NOT "_u_1"."srce_syst_m" IS NULL
    AND NOT "upi"."btch_run_d" IS NULL
  QUALIFY
    RANK() OVER (PARTITION BY "upi"."trgt_m" ORDER BY "upi"."btch_run_d" DESC NULLS LAST) = 1
), "dt2" AS (
  SELECT
    "ubi"."srce_syst_m" AS "srce_syst_m"
  FROM "%%vtech%%"."util_btch_isac" AS "ubi"
  WHERE
    "ubi"."btch_run_d" >= :EXTR_D
    AND "ubi"."btch_stus_c" = 'COMT'
    AND "ubi"."srce_syst_m" = 'PRTF_TECH'
  QUALIFY
    RANK() OVER (ORDER BY "ubi"."btch_run_d" DESC NULLS LAST, "ubi"."stus_chng_s" DESC NULLS LAST) = 1
), "srces" AS (
  SELECT
    1 AS "srce_fnd"
  FROM "dt" AS "dt"
  JOIN "%%vtech%%"."util_parm" AS "up2"
    ON "up2"."parm_m" = 'DERV_ACCT_PATY_SAPBP_SCHE_DEPN_SRCE_LOAD'
  HAVING
    COUNT("dt"."trgt_m") = MAX("up2"."parm_ltrl_n")
  UNION ALL
  SELECT
    1 AS "srce_fnd"
  FROM "dt2" AS "dt2"
  HAVING
    COUNT("dt2"."srce_syst_m") = 1
)
SELECT
  "srces"."srce_fnd" AS "srce_fnd"
FROM "srces" AS "srces"
JOIN "%%vtech%%"."util_parm" AS "up2"
  ON "up2"."parm_m" = 'DERV_ACCT_PATY_SCHE_DEPN_SRCE_LOAD'
GROUP BY
  "srces"."srce_fnd"
HAVING
  COUNT("srces"."srce_fnd") = MAX("up2"."parm_ltrl_n")
```

#### 📊 SQL Metadata:
- **Tables**: UTIL_BTCH_ISAC, UTIL_PARM, UTIL_PROS_ISAC
- **Columns**: BTCH_RUN_D, BTCH_STUS_C, COMT_F, PARM_LTRL_N, PARM_LTRL_STRG_X, PARM_M, SRCE_FND, SRCE_SYST_M, STUS_CHNG_S, TRGT_M
- **Functions**: NOT BTCH_RUN_D IS NULL, NOT BTCH_RUN_D IS NULL AND BTCH_RUN_D <> '0001-01-01', NOT BTCH_RUN_D IS NULL AND BTCH_RUN_D <> '0001-01-01' AND UPI.BTCH_RUN_D = :EXTR_D, NOT BTCH_RUN_D IS NULL AND BTCH_RUN_D <> '0001-01-01' AND UPI.BTCH_RUN_D = :EXTR_D AND UPI.TRGT_M IN (SELECT UP1.PARM_LTRL_STRG_X FROM %%VTECH%%.UTIL_PARM AS UP1 WHERE UP1.PARM_M = 'DERV_ACCT_PATY_SAPBP_SCHE_DEPN_TABL'), NOT BTCH_RUN_D IS NULL AND BTCH_RUN_D <> '0001-01-01' AND UPI.BTCH_RUN_D = :EXTR_D AND UPI.TRGT_M IN (SELECT UP1.PARM_LTRL_STRG_X FROM %%VTECH%%.UTIL_PARM AS UP1 WHERE UP1.PARM_M = 'DERV_ACCT_PATY_SAPBP_SCHE_DEPN_TABL') AND UPI.SRCE_SYST_M IN (SELECT UP1.PARM_LTRL_STRG_X AS SRCE_SYST_M FROM %%VTECH%%.UTIL_PARM AS UP1 WHERE UP1.PARM_M = 'DERV_ACCT_PATY_SAPBP_SCHE_DEPN_SRCE'), RANK, SRCE_FND, SRCE_SYST_M, TRGT_M, UBI.BTCH_STUS_C = 'COMT', UBI.BTCH_STUS_C = 'COMT' AND UBI.SRCE_SYST_M = 'PRTF_TECH'
- **Window_Functions**: RANK

## Migration Recommendations

### Suggested Migration Strategy
**High complexity** - Break into multiple models, use DCF full framework

### DCF Mapping
- **Error Handling**: Convert `.IF ERRORCODE` patterns to DCF error handling macros
- **File Operations**: Replace IMPORT/EXPORT with dbt seeds or external tables
- **Statistics**: Replace COLLECT STATS with Snowflake post-hooks
- **Stored Procedures**: Map CALL statements to DCF stored procedure macros

---

*Generated by BTEQ Parser Service - 2025-08-20 12:39:23*
