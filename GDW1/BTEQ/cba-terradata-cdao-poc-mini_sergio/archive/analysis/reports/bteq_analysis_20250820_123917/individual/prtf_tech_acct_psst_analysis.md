# prtf_tech_acct_psst.sql - BTEQ Analysis

## File Overview
- **File Name**: prtf_tech_acct_psst.sql
- **Analysis Status**: ✅ Success
- **Control Statements**: 12
- **SQL Blocks**: 6

## Control Flow Analysis

| Line | Type | Statement |
|------|------|-----------|
| 1 | RUN | `.RUN FILE=%%BTEQ_LOGON_SCRIPT%%` |
| 2 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 34 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 111 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 119 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 199 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 203 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 229 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 257 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 264 | LOGOFF | `.LOGOFF` |
| 266 | LABEL | `.LABEL EXITERR` |
| 268 | LOGOFF | `.LOGOFF` |

## SQL Blocks Analysis

### SQL Block 1 (Lines 32-33)
- **Complexity Score**: 6
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
Delete from %%STARDATADB%%.DERV_PRTF_ACCT_INT_GRUP_PSST All
;
```

#### ❄️ Converted Snowflake SQL:
```sql
DELETE FROM %%STARDATADB%%.DERV_PRTF_ACCT_INT_GRUP_PSST AS All
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
DELETE FROM "%%stardatadb%%"."derv_prtf_acct_int_grup_psst" AS "all"
```

#### 📊 SQL Metadata:
- **Tables**: DERV_PRTF_ACCT_INT_GRUP_PSST

### SQL Block 2 (Lines 36-110)
- **Complexity Score**: 263
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 5

#### 📝 Original Teradata SQL:
```sql
Insert into %%STARDATADB%%.DERV_PRTF_ACCT_INT_GRUP_PSST
Select
	 DT2.INT_GRUP_I       
	,DT2.ACCT_I
	,DT2.EFFT_D
	,DT2.EXPY_D
	,DT2.VALD_FROM_D
	,DT2.VALD_TO_D
	,DT2.PERD_D
	,DT2.REL_C
	,DT2.SRCE_SYST_C
	,ROW_NUMBER() OVER (Partition By DT2.ACCT_I, DT2.REL_C Order by DT2.PERD_D ) as GRUP_N
	,DT2.ROW_SECU_ACCS_C
  ,0                          as PROS_KEY_I
From
	(
		Select Distinct
			 DT1.INT_GRUP_I       
			,DT1.ACCT_I
			,DT1.EFFT_D
			,DT1.EXPY_D
			,DT1.VALD_FROM_D
			,DT1.VALD_TO_D
			,C.CALENDAR_DATE as PERD_D
			,DT1.REL_C
			,DT1.SRCE_SYST_C
			,DT1.ROW_SECU_ACCS_C
		From
			(
				Select
					 A.INT_GRUP_I
					,A.ACCT_I
					,A.REL_C
					,A.EFFT_D
					,A.EXPY_D
					,A.VALD_FROM_D        
					,A.VALD_TO_D
          ,A.JOIN_FROM_D
          ,A.JOIN_TO_D
					,A.SRCE_SYST_C
					,A.ROW_SECU_ACCS_C
				From
					%%VTECH%%.DERV_PRTF_ACCT_PSST A
					Inner Join %%VTECH%%.DERV_PRTF_ACCT_PSST B
					On A.ACCT_I = B.ACCT_I
					and A.REL_C = B.REL_C
          -- Nathan bug fix
					--And A.INT_GRUP_I <> B.INT_GRUP_I
					--And (A.JOIN_FROM_D,A.JOIN_TO_D) Overlaps (B.JOIN_FROM_D, B.JOIN_TO_D)

          And A.JOIN_TO_D >= B.JOIN_FROM_D
          And A.JOIN_FROM_D <= B.JOIN_TO_D

          And (
            --A.JOIN_FROM_D <> B.JOIN_FROM_D
            --Or A.JOIN_TO_D <> B.JOIN_TO_D
            A.EFFT_D <> B.EFFT_D
            Or B.EXPY_D <> B.EXPY_D
            -- Nathan bug fix
            Or A.INT_GRUP_I <> B.INT_GRUP_I
            Or A.PROS_KEY_I <> B.PROS_KEY_I
          )
					
			) DT1

			Inner Join %%VTECH%%.CALENDAR C
			--On C.CALENDAR_DATE between DT1.VALD_FROM_D and DT1.VALD_TO_D
			On C.CALENDAR_DATE between DT1.JOIN_FROM_D and DT1.JOIN_TO_D
			And C.CALENDAR_DATE between ADD_MONTHS((Current_Date - EXTRACT(DAY from CURRENT_DATE) +1 ),-39) and ADD_MONTHS(Current_Date, 1)

    -- Have to also order by INT_GRUP_I in case both have same EFFT_D
		Qualify Row_Number() Over( Partition By DT1.ACCT_I, DT1.REL_C, C.CALENDAR_DATE Order BY DT1.EFFT_D Desc, DT1.INT_GRUP_I Desc) = 1

	) DT2
;
```

#### ❄️ Converted Snowflake SQL:
```sql
INSERT INTO %%STARDATADB%%.DERV_PRTF_ACCT_INT_GRUP_PSST SELECT DT2.INT_GRUP_I, DT2.ACCT_I, DT2.EFFT_D, DT2.EXPY_D, DT2.VALD_FROM_D, DT2.VALD_TO_D, DT2.PERD_D, DT2.REL_C, DT2.SRCE_SYST_C, ROW_NUMBER() OVER (PARTITION BY DT2.ACCT_I, DT2.REL_C ORDER BY DT2.PERD_D NULLS FIRST) AS GRUP_N, DT2.ROW_SECU_ACCS_C, 0 AS PROS_KEY_I FROM (SELECT DISTINCT DT1.INT_GRUP_I, DT1.ACCT_I, DT1.EFFT_D, DT1.EXPY_D, DT1.VALD_FROM_D, DT1.VALD_TO_D, C.CALENDAR_DATE AS PERD_D, DT1.REL_C, DT1.SRCE_SYST_C, DT1.ROW_SECU_ACCS_C FROM (SELECT A.INT_GRUP_I, A.ACCT_I, A.REL_C, A.EFFT_D, A.EXPY_D, A.VALD_FROM_D, A.VALD_TO_D, A.JOIN_FROM_D, A.JOIN_TO_D, A.SRCE_SYST_C, A.ROW_SECU_ACCS_C FROM %%VTECH%%.DERV_PRTF_ACCT_PSST AS A INNER JOIN %%VTECH%%.DERV_PRTF_ACCT_PSST AS B ON A.ACCT_I = B.ACCT_I AND A.REL_C = B.REL_C AND A.JOIN_TO_D >= B.JOIN_FROM_D AND A.JOIN_FROM_D <= B.JOIN_TO_D AND (A.EFFT_D <> B.EFFT_D OR B.EXPY_D <> B.EXPY_D OR A.INT_GRUP_I <> B.INT_GRUP_I OR A.PROS_KEY_I <> B.PROS_KEY_I)) AS DT1 INNER JOIN %%VTECH%%.CALENDAR AS C ON C.CALENDAR_DATE BETWEEN DT1.JOIN_FROM_D AND DT1.JOIN_TO_D AND C.CALENDAR_DATE BETWEEN ADD_MONTHS((CURRENT_DATE - DATE_PART(DAY, CURRENT_DATE) + 1), -39) AND ADD_MONTHS(CURRENT_DATE, 1) QUALIFY ROW_NUMBER() OVER (PARTITION BY DT1.ACCT_I, DT1.REL_C, C.CALENDAR_DATE ORDER BY DT1.EFFT_D DESC NULLS LAST, DT1.INT_GRUP_I DESC NULLS LAST) = 1) AS DT2
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- QUALIFY clause
- ADD_MONTHS function
- EXTRACT with complex syntax
- ROW_NUMBER() OVER
- Variable substitution

#### ⚡ Optimized SQL:
```sql
INSERT INTO "%%stardatadb%%"."derv_prtf_acct_int_grup_psst"
WITH "dt2" AS (
  SELECT DISTINCT
    "a"."int_grup_i" AS "int_grup_i",
    "a"."acct_i" AS "acct_i",
    "a"."efft_d" AS "efft_d",
    "a"."expy_d" AS "expy_d",
    "a"."vald_from_d" AS "vald_from_d",
    "a"."vald_to_d" AS "vald_to_d",
    "c"."calendar_date" AS "perd_d",
    "a"."rel_c" AS "rel_c",
    "a"."srce_syst_c" AS "srce_syst_c",
    "a"."row_secu_accs_c" AS "row_secu_accs_c"
  FROM "%%vtech%%"."derv_prtf_acct_psst" AS "a"
  JOIN "%%vtech%%"."derv_prtf_acct_psst" AS "b"
    ON "a"."acct_i" = "b"."acct_i"
    AND (
      "a"."efft_d" <> "b"."efft_d"
      OR "a"."int_grup_i" <> "b"."int_grup_i"
      OR "a"."pros_key_i" <> "b"."pros_key_i"
      OR "b"."expy_d" <> "b"."expy_d"
    )
    AND "a"."join_from_d" <= "b"."join_to_d"
    AND "a"."join_to_d" >= "b"."join_from_d"
    AND "a"."rel_c" = "b"."rel_c"
  JOIN "%%vtech%%"."calendar" AS "c"
    ON "a"."join_from_d" <= "c"."calendar_date"
    AND "a"."join_to_d" >= "c"."calendar_date"
    AND "c"."calendar_date" <= ADD_MONTHS(CURRENT_DATE, 1)
    AND "c"."calendar_date" >= ADD_MONTHS((
      CURRENT_DATE - DATE_PART(DAY, CURRENT_DATE) + 1
    ), -39)
  QUALIFY
    ROW_NUMBER() OVER (
      PARTITION BY "a"."acct_i", "a"."rel_c", "c"."calendar_date"
      ORDER BY "a"."efft_d" DESC NULLS LAST, "a"."int_grup_i" DESC NULLS LAST
    ) = 1
)
SELECT
  "dt2"."int_grup_i" AS "int_grup_i",
  "dt2"."acct_i" AS "acct_i",
  "dt2"."efft_d" AS "efft_d",
  "dt2"."expy_d" AS "expy_d",
  "dt2"."vald_from_d" AS "vald_from_d",
  "dt2"."vald_to_d" AS "vald_to_d",
  "dt2"."perd_d" AS "perd_d",
  "dt2"."rel_c" AS "rel_c",
  "dt2"."srce_syst_c" AS "srce_syst_c",
  ROW_NUMBER() OVER (PARTITION BY "dt2"."acct_i", "dt2"."rel_c" ORDER BY "dt2"."perd_d" NULLS FIRST) AS "grup_n",
  "dt2"."row_secu_accs_c" AS "row_secu_accs_c",
  0 AS "pros_key_i"
FROM "dt2" AS "dt2"
```

#### 📊 SQL Metadata:
- **Tables**: CALENDAR, DERV_PRTF_ACCT_INT_GRUP_PSST, DERV_PRTF_ACCT_PSST
- **Columns**: ACCT_I, CALENDAR_DATE, EFFT_D, EXPY_D, INT_GRUP_I, JOIN_FROM_D, JOIN_TO_D, PERD_D, PROS_KEY_I, REL_C, ROW_SECU_ACCS_C, SRCE_SYST_C, VALD_FROM_D, VALD_TO_D
- **Functions**: (CURRENT_DATE - EXTRACT(DAY FROM CURRENT_DATE) + 1), A.ACCT_I = B.ACCT_I, A.ACCT_I = B.ACCT_I AND A.REL_C = B.REL_C, A.ACCT_I = B.ACCT_I AND A.REL_C = B.REL_C AND A.JOIN_TO_D >= B.JOIN_FROM_D, A.ACCT_I = B.ACCT_I AND A.REL_C = B.REL_C AND A.JOIN_TO_D >= B.JOIN_FROM_D AND A.JOIN_FROM_D <= B.JOIN_TO_D, A.EFFT_D <> B.EFFT_D, A.EFFT_D <> B.EFFT_D OR B.EXPY_D <> B.EXPY_D, A.EFFT_D <> B.EFFT_D OR B.EXPY_D <> B.EXPY_D OR A.INT_GRUP_I <> B.INT_GRUP_I, C.CALENDAR_DATE BETWEEN DT1.JOIN_FROM_D AND DT1.JOIN_TO_D, CURRENT_DATE, DAY, None
- **Window_Functions**: None

### SQL Block 3 (Lines 117-118)
- **Complexity Score**: 6
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
Delete from %%STARDATADB%%.DERV_PRTF_ACCT_HIST_PSST All
;
```

#### ❄️ Converted Snowflake SQL:
```sql
DELETE FROM %%STARDATADB%%.DERV_PRTF_ACCT_HIST_PSST AS All
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
DELETE FROM "%%stardatadb%%"."derv_prtf_acct_hist_psst" AS "all"
```

#### 📊 SQL Metadata:
- **Tables**: DERV_PRTF_ACCT_HIST_PSST

### SQL Block 4 (Lines 121-198)
- **Complexity Score**: 314
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
Insert into %%STARDATADB%%.DERV_PRTF_ACCT_HIST_PSST
/* Working History for ACCT data */
Select      
   DT3.INT_GRUP_I                    
  ,DT3.ACCT_I                        
  ,DT3.REL_C                         
  ,DT3.JOIN_FROM_D                   
  ,DT3.JOIN_TO_D                     
  ,DT3.VALD_FROM_D                   
  ,DT3.VALD_TO_D                     
  ,DT3.EFFT_D                        
  ,DT3.EXPY_D                        
  ,DT3.SRCE_SYST_C                   
  ,DT3.ROW_SECU_ACCS_C              
From
	(
    Select
       DT2.INT_GRUP_I
      ,DT2.ACCT_I
      ,DT2.REL_C
      ,DT2.EFFT_D
      ,DT2.EXPY_D
      ,DT2.VALD_FROM_D
      ,DT2.VALD_TO_D
      ,DT2.PERD_D
      ,MIN(DT2.PERD_D) Over (Partition By DT2.ACCT_I, DT2.REL_C, DT2.GRUP_N) as JOIN_FROM_D
      ,MAX(DT2.PERD_D) Over (Partition by DT2.ACCT_I, DT2.REL_C, DT2.GRUP_N )  as JOIN_TO_D
      ,DT2.SRCE_SYST_C
      ,DT2.GRUP_N
      ,DT2.ROW_SECU_ACCS_C
    From
      (
        Select
           PAIG.INT_GRUP_I
          ,PAIG.ACCT_I
          ,PAIG.REL_C
          ,PAIG.PERD_D
          ,PAIG.EFFT_D
          ,PAIG.EXPY_D
          ,PAIG.VALD_FROM_D
          ,PAIG.VALD_TO_D
          ,PAIG.SRCE_SYST_C
          ,MAX(Coalesce( DT1.ROW_N, 0 )) Over (Partition By PAIG.ACCT_I, PAIG.INT_GRUP_I, PAIG.PERD_D) as GRUP_N        
          ,PAIG.ROW_SECU_ACCS_C
         From
          %%VTECH%%.DERV_PRTF_ACCT_INT_GRUP_PSST PAIG
          Left Join (
            Select
               A.INT_GRUP_I
              ,A.ACCT_I
              ,A.REL_C
              ,A.ROW_N
              ,A.PERD_D
              ,A.EFFT_D
              ,A.EXPY_D
            From
              %%VTECH%%.DERV_PRTF_ACCT_INT_GRUP_PSST A
              Inner Join %%VTECH%%.DERV_PRTF_ACCT_INT_GRUP_PSST B
              On A.ACCT_I = B.ACCT_I
              And A.REL_C = B.REL_C
              And A.ROW_N = B.ROW_N + 1
              And (
                A.EFFT_D <> B.EFFT_D
                Or A.INT_GRUP_I <> B.INT_GRUP_I
              )
          ) DT1
          On DT1.INT_GRUP_I = PAIG.INT_GRUP_I
          And DT1.ACCT_I = PAIG.ACCT_I
          And DT1.REL_C = PAIG.REL_C
          And DT1.EFFT_D = PAIG.EFFT_D
          And DT1.EXPY_D = PAIG.EXPY_D
          And PAIG.ROW_N >= DT1.ROW_N
          And DT1.PERD_D <= PAIG.PERD_D
      ) DT2

  ) DT3
Group By 1,2,3,4,5,6,7,8,9,10,11
;
```

#### ❄️ Converted Snowflake SQL:
```sql
INSERT INTO %%STARDATADB%%.DERV_PRTF_ACCT_HIST_PSST SELECT DT3.INT_GRUP_I, DT3.ACCT_I, DT3.REL_C, DT3.JOIN_FROM_D, DT3.JOIN_TO_D, DT3.VALD_FROM_D, DT3.VALD_TO_D, DT3.EFFT_D, DT3.EXPY_D, DT3.SRCE_SYST_C, DT3.ROW_SECU_ACCS_C FROM (SELECT DT2.INT_GRUP_I, DT2.ACCT_I, DT2.REL_C, DT2.EFFT_D, DT2.EXPY_D, DT2.VALD_FROM_D, DT2.VALD_TO_D, DT2.PERD_D, MIN(DT2.PERD_D) OVER (PARTITION BY DT2.ACCT_I, DT2.REL_C, DT2.GRUP_N) AS JOIN_FROM_D, MAX(DT2.PERD_D) OVER (PARTITION BY DT2.ACCT_I, DT2.REL_C, DT2.GRUP_N) AS JOIN_TO_D, DT2.SRCE_SYST_C, DT2.GRUP_N, DT2.ROW_SECU_ACCS_C FROM (SELECT PAIG.INT_GRUP_I, PAIG.ACCT_I, PAIG.REL_C, PAIG.PERD_D, PAIG.EFFT_D, PAIG.EXPY_D, PAIG.VALD_FROM_D, PAIG.VALD_TO_D, PAIG.SRCE_SYST_C, MAX(COALESCE(DT1.ROW_N, 0)) OVER (PARTITION BY PAIG.ACCT_I, PAIG.INT_GRUP_I, PAIG.PERD_D) AS GRUP_N, PAIG.ROW_SECU_ACCS_C FROM %%VTECH%%.DERV_PRTF_ACCT_INT_GRUP_PSST AS PAIG LEFT JOIN (SELECT A.INT_GRUP_I, A.ACCT_I, A.REL_C, A.ROW_N, A.PERD_D, A.EFFT_D, A.EXPY_D FROM %%VTECH%%.DERV_PRTF_ACCT_INT_GRUP_PSST AS A INNER JOIN %%VTECH%%.DERV_PRTF_ACCT_INT_GRUP_PSST AS B ON A.ACCT_I = B.ACCT_I AND A.REL_C = B.REL_C AND A.ROW_N = B.ROW_N + 1 AND (A.EFFT_D <> B.EFFT_D OR A.INT_GRUP_I <> B.INT_GRUP_I)) AS DT1 ON DT1.INT_GRUP_I = PAIG.INT_GRUP_I AND DT1.ACCT_I = PAIG.ACCT_I AND DT1.REL_C = PAIG.REL_C AND DT1.EFFT_D = PAIG.EFFT_D AND DT1.EXPY_D = PAIG.EXPY_D AND PAIG.ROW_N >= DT1.ROW_N AND DT1.PERD_D <= PAIG.PERD_D) AS DT2) AS DT3 GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
INSERT INTO "%%stardatadb%%"."derv_prtf_acct_hist_psst"
WITH "dt2" AS (
  SELECT
    "paig"."int_grup_i" AS "int_grup_i",
    "paig"."acct_i" AS "acct_i",
    "paig"."rel_c" AS "rel_c",
    "paig"."perd_d" AS "perd_d",
    "paig"."efft_d" AS "efft_d",
    "paig"."expy_d" AS "expy_d",
    "paig"."vald_from_d" AS "vald_from_d",
    "paig"."vald_to_d" AS "vald_to_d",
    "paig"."srce_syst_c" AS "srce_syst_c",
    MAX(COALESCE("a"."row_n", 0)) OVER (PARTITION BY "paig"."acct_i", "paig"."int_grup_i", "paig"."perd_d") AS "grup_n",
    "paig"."row_secu_accs_c" AS "row_secu_accs_c"
  FROM "%%vtech%%"."derv_prtf_acct_int_grup_psst" AS "paig"
  LEFT JOIN "%%vtech%%"."derv_prtf_acct_int_grup_psst" AS "a"
    ON "a"."acct_i" = "paig"."acct_i"
    AND "a"."efft_d" = "paig"."efft_d"
    AND "a"."expy_d" = "paig"."expy_d"
    AND "a"."int_grup_i" = "paig"."int_grup_i"
    AND "a"."perd_d" <= "paig"."perd_d"
    AND "a"."rel_c" = "paig"."rel_c"
    AND "a"."row_n" <= "paig"."row_n"
  JOIN "%%vtech%%"."derv_prtf_acct_int_grup_psst" AS "b"
    ON "a"."acct_i" = "b"."acct_i"
    AND (
      "a"."efft_d" <> "b"."efft_d" OR "a"."int_grup_i" <> "b"."int_grup_i"
    )
    AND "a"."rel_c" = "b"."rel_c"
    AND "a"."row_n" = "b"."row_n" + 1
), "dt3" AS (
  SELECT
    "dt2"."int_grup_i" AS "int_grup_i",
    "dt2"."acct_i" AS "acct_i",
    "dt2"."rel_c" AS "rel_c",
    "dt2"."efft_d" AS "efft_d",
    "dt2"."expy_d" AS "expy_d",
    "dt2"."vald_from_d" AS "vald_from_d",
    "dt2"."vald_to_d" AS "vald_to_d",
    MIN("dt2"."perd_d") OVER (PARTITION BY "dt2"."acct_i", "dt2"."rel_c", "dt2"."grup_n") AS "join_from_d",
    MAX("dt2"."perd_d") OVER (PARTITION BY "dt2"."acct_i", "dt2"."rel_c", "dt2"."grup_n") AS "join_to_d",
    "dt2"."srce_syst_c" AS "srce_syst_c",
    "dt2"."row_secu_accs_c" AS "row_secu_accs_c"
  FROM "dt2" AS "dt2"
)
SELECT
  "dt3"."int_grup_i" AS "int_grup_i",
  "dt3"."acct_i" AS "acct_i",
  "dt3"."rel_c" AS "rel_c",
  "dt3"."join_from_d" AS "join_from_d",
  "dt3"."join_to_d" AS "join_to_d",
  "dt3"."vald_from_d" AS "vald_from_d",
  "dt3"."vald_to_d" AS "vald_to_d",
  "dt3"."efft_d" AS "efft_d",
  "dt3"."expy_d" AS "expy_d",
  "dt3"."srce_syst_c" AS "srce_syst_c",
  "dt3"."row_secu_accs_c" AS "row_secu_accs_c"
FROM "dt3" AS "dt3"
GROUP BY
  "dt3"."int_grup_i",
  "dt3"."acct_i",
  "dt3"."rel_c",
  "dt3"."join_from_d",
  "dt3"."join_to_d",
  "dt3"."vald_from_d",
  "dt3"."vald_to_d",
  "dt3"."efft_d",
  "dt3"."expy_d",
  "dt3"."srce_syst_c",
  "dt3"."row_secu_accs_c"
```

#### 📊 SQL Metadata:
- **Tables**: DERV_PRTF_ACCT_HIST_PSST, DERV_PRTF_ACCT_INT_GRUP_PSST
- **Columns**: ACCT_I, EFFT_D, EXPY_D, GRUP_N, INT_GRUP_I, JOIN_FROM_D, JOIN_TO_D, PERD_D, REL_C, ROW_N, ROW_SECU_ACCS_C, SRCE_SYST_C, VALD_FROM_D, VALD_TO_D
- **Functions**: A.ACCT_I = B.ACCT_I, A.ACCT_I = B.ACCT_I AND A.REL_C = B.REL_C, A.ACCT_I = B.ACCT_I AND A.REL_C = B.REL_C AND A.ROW_N = B.ROW_N + 1, A.EFFT_D <> B.EFFT_D, COALESCE(DT1.ROW_N, 0), DT1.INT_GRUP_I = PAIG.INT_GRUP_I, DT1.INT_GRUP_I = PAIG.INT_GRUP_I AND DT1.ACCT_I = PAIG.ACCT_I, DT1.INT_GRUP_I = PAIG.INT_GRUP_I AND DT1.ACCT_I = PAIG.ACCT_I AND DT1.REL_C = PAIG.REL_C, DT1.INT_GRUP_I = PAIG.INT_GRUP_I AND DT1.ACCT_I = PAIG.ACCT_I AND DT1.REL_C = PAIG.REL_C AND DT1.EFFT_D = PAIG.EFFT_D, DT1.INT_GRUP_I = PAIG.INT_GRUP_I AND DT1.ACCT_I = PAIG.ACCT_I AND DT1.REL_C = PAIG.REL_C AND DT1.EFFT_D = PAIG.EFFT_D AND DT1.EXPY_D = PAIG.EXPY_D, DT1.INT_GRUP_I = PAIG.INT_GRUP_I AND DT1.ACCT_I = PAIG.ACCT_I AND DT1.REL_C = PAIG.REL_C AND DT1.EFFT_D = PAIG.EFFT_D AND DT1.EXPY_D = PAIG.EXPY_D AND PAIG.ROW_N >= DT1.ROW_N, DT1.ROW_N, DT2.PERD_D
- **Window_Functions**: COALESCE(DT1.ROW_N, 0), DT2.PERD_D

### SQL Block 5 (Lines 217-228)
- **Complexity Score**: 46
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
Delete
  A
From
	%%STARDATADB%%.DERV_PRTF_ACCT_PSST A
  ,%%VTECH%%.DERV_PRTF_ACCT_HIST_PSST B
Where
  A.ACCT_I = B.ACCT_I
  And A.REL_C = B.REL_C
  --And (A.JOIN_FROM_D,A.JOIN_TO_D) Overlaps (B.JOIN_FROM_D, B.JOIN_TO_D)
  And A.JOIN_TO_D >= B.JOIN_FROM_D
  And A.JOIN_FROM_D <= B.JOIN_TO_D 
;
```

#### ❄️ Converted Snowflake SQL:
```sql
DELETE A FROM %%STARDATADB%%.DERV_PRTF_ACCT_PSST AS A, %%VTECH%%.DERV_PRTF_ACCT_HIST_PSST AS B WHERE A.ACCT_I = B.ACCT_I AND A.REL_C = B.REL_C AND A.JOIN_TO_D >= B.JOIN_FROM_D AND A.JOIN_FROM_D <= B.JOIN_TO_D
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
DELETE   "a" FROM "%%stardatadb%%"."derv_prtf_acct_psst" AS "a"
  CROSS JOIN "%%vtech%%"."derv_prtf_acct_hist_psst" AS "b"
WHERE
  "a"."acct_i" = "b"."acct_i"
  AND "a"."join_from_d" <= "b"."join_to_d"
  AND "a"."join_to_d" >= "b"."join_from_d"
  AND "a"."rel_c" = "b"."rel_c"
```

#### 📊 SQL Metadata:
- **Tables**: A, DERV_PRTF_ACCT_HIST_PSST, DERV_PRTF_ACCT_PSST
- **Columns**: ACCT_I, JOIN_FROM_D, JOIN_TO_D, REL_C
- **Functions**: A.ACCT_I = B.ACCT_I, A.ACCT_I = B.ACCT_I AND A.REL_C = B.REL_C, A.ACCT_I = B.ACCT_I AND A.REL_C = B.REL_C AND A.JOIN_TO_D >= B.JOIN_FROM_D

### SQL Block 6 (Lines 236-256)
- **Complexity Score**: 0
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ❌ Invalid
- **Teradata Features**: 2

#### 📝 Original Teradata SQL:
```sql
Insert into %%STARDATADB%%.DERV_PRTF_ACCT_PSST
Select
   INT_GRUP_I                    
  ,ACCT_I                        
  ,REL_C    
  ,JOIN_FROM_D
  ,(Case
      WHEN JOIN_TO_D = ADD_MONTHS(CURRENT_DATE, 1) Then ('9999-12-31'(Date,format'yyyy-mm-dd'))             
      ELSE JOIN_TO_D
    End
    ) as JOIN_TO_D   
  ,VALD_FROM_D                   
  ,VALD_TO_D                     
  ,EFFT_D                        
  ,EXPY_D                        
  ,SRCE_SYST_C 
  ,0 as PROS_KEY_EFFT_I              
  ,ROW_SECU_ACCS_C               
From 
  %%VTECH%%.DERV_PRTF_ACCT_HIST_PSST
;
```

#### ❄️ Converted Snowflake SQL:
```sql
Insert into %%STARDATADB%%.DERV_PRTF_ACCT_PSST
Select
   INT_GRUP_I                    
  ,ACCT_I                        
  ,REL_C    
  ,JOIN_FROM_D
  ,(Case
      WHEN JOIN_TO_D = ADD_MONTHS(CURRENT_DATE, 1) Then ('9999-12-31'(Date,format'yyyy-mm-dd'))             
      ELSE JOIN_TO_D
    End
    ) as JOIN_TO_D   
  ,VALD_FROM_D                   
  ,VALD_TO_D                     
  ,EFFT_D                        
  ,EXPY_D                        
  ,SRCE_SYST_C 
  ,0 as PROS_KEY_EFFT_I              
  ,ROW_SECU_ACCS_C               
From 
  %%VTECH%%.DERV_PRTF_ACCT_HIST_PSST
;
```

#### 🔍 Syntax Validation Details:
- **Valid**: ❌

#### 🎯 Teradata Features Detected:
- ADD_MONTHS function
- Variable substitution

## Migration Recommendations

### Suggested Migration Strategy
**High complexity** - Break into multiple models, use DCF full framework

### DCF Mapping
- **Error Handling**: Convert `.IF ERRORCODE` patterns to DCF error handling macros
- **File Operations**: Replace IMPORT/EXPORT with dbt seeds or external tables
- **Statistics**: Replace COLLECT STATS with Snowflake post-hooks
- **Stored Procedures**: Map CALL statements to DCF stored procedure macros

---

*Generated by BTEQ Parser Service - 2025-08-20 12:39:20*
