# prtf_tech_acct_int_grup_psst.sql - BTEQ Analysis

## File Overview
- **File Name**: prtf_tech_acct_int_grup_psst.sql
- **Analysis Status**: ✅ Success
- **Control Statements**: 16
- **SQL Blocks**: 8

## Control Flow Analysis

| Line | Type | Statement |
|------|------|-----------|
| 1 | RUN | `.RUN FILE=%%BTEQ_LOGON_SCRIPT%%` |
| 2 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 32 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 69 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 74 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 82 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 154 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 158 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 166 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 224 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 228 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 248 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 276 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 280 | LOGOFF | `.LOGOFF` |
| 282 | LABEL | `.LABEL EXITERR` |
| 284 | LOGOFF | `.LOGOFF` |

## SQL Blocks Analysis

### SQL Block 1 (Lines 30-31)
- **Complexity Score**: 4
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
Delete from %%STARDATADB%%.DERV_PRTF_ACCT_PSST
;
```

#### ❄️ Converted Snowflake SQL:
```sql
DELETE FROM %%STARDATADB%%.DERV_PRTF_ACCT_PSST
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
DELETE FROM "%%stardatadb%%"."derv_prtf_acct_psst"
```

#### 📊 SQL Metadata:
- **Tables**: DERV_PRTF_ACCT_PSST

### SQL Block 2 (Lines 34-68)
- **Complexity Score**: 126
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
Insert into %%STARDATADB%%.DERV_PRTF_ACCT_PSST
Select
   AIG.INT_GRUP_I               as INT_GRUP_I
  ,AIG.ACCT_I                   as ACCT_I
  ,AIG.REL_C
  ,AIG.VALD_FROM_D              as JOIN_FROM_D
  ,AIG.VALD_TO_D			          as JOIN_TO_D
  ,AIG.VALD_FROM_D
  ,AIG.VALD_TO_D
  ,AIG.EFFT_D
  ,AIG.EXPY_D
  ,AIG.SRCE_SYST_C
  ,AIG.PROS_KEY_EFFT_I
  ,AIG.ROW_SECU_ACCS_C
From
  %%VTECH%%.ACCT_INT_GRUP AIG

  /* Add the GRD filter to reduce the data */
  INNER JOIN %%VTECH%%.INT_GRUP IG
  ON IG.INT_GRUP_I = AIG.INT_GRUP_I
    
  INNER JOIN %%VTECH%%.GRD_PRTF_TYPE_ENHC_HIST_PSST GPTE  
  ON GPTE.PRTF_TYPE_C = IG.INT_GRUP_TYPE_C

  -- Overlaps
  --AND (GPTE.VALD_FROM_D,GPTE.VALD_TO_D) Overlaps (IG.CRAT_D,IG.VALD_TO_D)
  And GPTE.VALD_TO_D >= IG.CRAT_D
  And GPTE.VALD_FROM_D <= IG.VALD_TO_D

  --AND (GPTE.VALD_FROM_D,GPTE.VALD_TO_D) Overlaps (AIG.VALD_FROM_D,AIG.VALD_TO_D)
  And GPTE.VALD_TO_D >= AIG.VALD_FROM_D
  And GPTE.VALD_FROM_D <= AIG.VALD_TO_D
   
Group By 1,2,3,4,5,6,7,8,9,10,11,12
;
```

#### ❄️ Converted Snowflake SQL:
```sql
INSERT INTO %%STARDATADB%%.DERV_PRTF_ACCT_PSST SELECT AIG.INT_GRUP_I AS INT_GRUP_I, AIG.ACCT_I AS ACCT_I, AIG.REL_C, AIG.VALD_FROM_D AS JOIN_FROM_D, AIG.VALD_TO_D AS JOIN_TO_D, AIG.VALD_FROM_D, AIG.VALD_TO_D, AIG.EFFT_D, AIG.EXPY_D, AIG.SRCE_SYST_C, AIG.PROS_KEY_EFFT_I, AIG.ROW_SECU_ACCS_C FROM %%VTECH%%.ACCT_INT_GRUP AS AIG INNER JOIN %%VTECH%%.INT_GRUP AS IG ON IG.INT_GRUP_I = AIG.INT_GRUP_I INNER JOIN %%VTECH%%.GRD_PRTF_TYPE_ENHC_HIST_PSST AS GPTE ON GPTE.PRTF_TYPE_C = IG.INT_GRUP_TYPE_C AND GPTE.VALD_TO_D >= IG.CRAT_D AND GPTE.VALD_FROM_D <= IG.VALD_TO_D AND GPTE.VALD_TO_D >= AIG.VALD_FROM_D AND GPTE.VALD_FROM_D <= AIG.VALD_TO_D GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
INSERT INTO "%%stardatadb%%"."derv_prtf_acct_psst"
SELECT
  "aig"."int_grup_i" AS "int_grup_i",
  "aig"."acct_i" AS "acct_i",
  "aig"."rel_c" AS "rel_c",
  "aig"."vald_from_d" AS "join_from_d",
  "aig"."vald_to_d" AS "join_to_d",
  "aig"."vald_from_d" AS "vald_from_d",
  "aig"."vald_to_d" AS "vald_to_d",
  "aig"."efft_d" AS "efft_d",
  "aig"."expy_d" AS "expy_d",
  "aig"."srce_syst_c" AS "srce_syst_c",
  "aig"."pros_key_efft_i" AS "pros_key_efft_i",
  "aig"."row_secu_accs_c" AS "row_secu_accs_c"
FROM "%%vtech%%"."acct_int_grup" AS "aig"
JOIN "%%vtech%%"."int_grup" AS "ig"
  ON "aig"."int_grup_i" = "ig"."int_grup_i"
JOIN "%%vtech%%"."grd_prtf_type_enhc_hist_psst" AS "gpte"
  ON "aig"."vald_from_d" <= "gpte"."vald_to_d"
  AND "aig"."vald_to_d" >= "gpte"."vald_from_d"
  AND "gpte"."prtf_type_c" = "ig"."int_grup_type_c"
  AND "gpte"."vald_from_d" <= "ig"."vald_to_d"
  AND "gpte"."vald_to_d" >= "ig"."crat_d"
GROUP BY
  "aig"."int_grup_i",
  "aig"."acct_i",
  "aig"."rel_c",
  "aig"."vald_from_d",
  "aig"."vald_to_d",
  "aig"."vald_from_d",
  "aig"."vald_to_d",
  "aig"."efft_d",
  "aig"."expy_d",
  "aig"."srce_syst_c",
  "aig"."pros_key_efft_i",
  "aig"."row_secu_accs_c"
```

#### 📊 SQL Metadata:
- **Tables**: ACCT_INT_GRUP, DERV_PRTF_ACCT_PSST, GRD_PRTF_TYPE_ENHC_HIST_PSST, INT_GRUP
- **Columns**: ACCT_I, CRAT_D, EFFT_D, EXPY_D, INT_GRUP_I, INT_GRUP_TYPE_C, PROS_KEY_EFFT_I, PRTF_TYPE_C, REL_C, ROW_SECU_ACCS_C, SRCE_SYST_C, VALD_FROM_D, VALD_TO_D
- **Functions**: GPTE.PRTF_TYPE_C = IG.INT_GRUP_TYPE_C, GPTE.PRTF_TYPE_C = IG.INT_GRUP_TYPE_C AND GPTE.VALD_TO_D >= IG.CRAT_D, GPTE.PRTF_TYPE_C = IG.INT_GRUP_TYPE_C AND GPTE.VALD_TO_D >= IG.CRAT_D AND GPTE.VALD_FROM_D <= IG.VALD_TO_D, GPTE.PRTF_TYPE_C = IG.INT_GRUP_TYPE_C AND GPTE.VALD_TO_D >= IG.CRAT_D AND GPTE.VALD_FROM_D <= IG.VALD_TO_D AND GPTE.VALD_TO_D >= AIG.VALD_FROM_D

### SQL Block 3 (Lines 80-81)
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

### SQL Block 4 (Lines 84-153)
- **Complexity Score**: 258
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
	,ROW_NUMBER() OVER (Partition By DT2.INT_GRUP_I, DT2.ACCT_I Order by DT2.PERD_D, DT2.REL_C ) as GRUP_N
	,DT2.ROW_SECU_ACCS_C
  ,0                          as PROS_KEY_I
From
	(
		Select
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
					And A.INT_GRUP_I = B.INT_GRUP_I
					--And (A.JOIN_FROM_D,A.JOIN_TO_D) overlaps (B.JOIN_FROM_D,B.JOIN_TO_D)
          And A.JOIN_TO_D >= B.JOIN_FROM_D
          And A.JOIN_FROM_D <= B.JOIN_TO_D

          And (
            --A.JOIN_FROM_D <> B.JOIN_FROM_D
            --Or A.JOIN_TO_D <> B.JOIN_TO_D
            -- New to fix extra Nathan
            A.EFFT_D <> B.EFFT_D
            Or A.EXPY_D <> B.EXPY_D
            Or A.PROS_KEY_I <> B.PROS_KEY_I
          )
 				
			) DT1

			Inner Join %%VTECH%%.CALENDAR C
			--On C.CALENDAR_DATE between DT1.VALD_FROM_D and DT1.VALD_TO_D
			On C.CALENDAR_DATE between DT1.JOIN_FROM_D and DT1.JOIN_TO_D
			And C.CALENDAR_DATE between ADD_MONTHS((Current_Date - EXTRACT(DAY from CURRENT_DATE) +1 ),-39) and ADD_MONTHS(Current_Date, 1)

		Qualify Row_Number() Over( Partition By DT1.ACCT_I, DT1.INT_GRUP_I, C.CALENDAR_DATE Order BY DT1.EFFT_D Desc, DT1.REL_C Desc) = 1

	) DT2
;
```

#### ❄️ Converted Snowflake SQL:
```sql
INSERT INTO %%STARDATADB%%.DERV_PRTF_ACCT_INT_GRUP_PSST SELECT DT2.INT_GRUP_I, DT2.ACCT_I, DT2.EFFT_D, DT2.EXPY_D, DT2.VALD_FROM_D, DT2.VALD_TO_D, DT2.PERD_D, DT2.REL_C, DT2.SRCE_SYST_C, ROW_NUMBER() OVER (PARTITION BY DT2.INT_GRUP_I, DT2.ACCT_I ORDER BY DT2.PERD_D NULLS FIRST, DT2.REL_C NULLS FIRST) AS GRUP_N, DT2.ROW_SECU_ACCS_C, 0 AS PROS_KEY_I FROM (SELECT DT1.INT_GRUP_I, DT1.ACCT_I, DT1.EFFT_D, DT1.EXPY_D, DT1.VALD_FROM_D, DT1.VALD_TO_D, C.CALENDAR_DATE AS PERD_D, DT1.REL_C, DT1.SRCE_SYST_C, DT1.ROW_SECU_ACCS_C FROM (SELECT A.INT_GRUP_I, A.ACCT_I, A.REL_C, A.EFFT_D, A.EXPY_D, A.VALD_FROM_D, A.VALD_TO_D, A.JOIN_FROM_D, A.JOIN_TO_D, A.SRCE_SYST_C, A.ROW_SECU_ACCS_C FROM %%VTECH%%.DERV_PRTF_ACCT_PSST AS A INNER JOIN %%VTECH%%.DERV_PRTF_ACCT_PSST AS B ON A.ACCT_I = B.ACCT_I AND A.INT_GRUP_I = B.INT_GRUP_I AND A.JOIN_TO_D >= B.JOIN_FROM_D AND A.JOIN_FROM_D <= B.JOIN_TO_D AND (A.EFFT_D <> B.EFFT_D OR A.EXPY_D <> B.EXPY_D OR A.PROS_KEY_I <> B.PROS_KEY_I)) AS DT1 INNER JOIN %%VTECH%%.CALENDAR AS C ON C.CALENDAR_DATE BETWEEN DT1.JOIN_FROM_D AND DT1.JOIN_TO_D AND C.CALENDAR_DATE BETWEEN ADD_MONTHS((CURRENT_DATE - DATE_PART(DAY, CURRENT_DATE) + 1), -39) AND ADD_MONTHS(CURRENT_DATE, 1) QUALIFY ROW_NUMBER() OVER (PARTITION BY DT1.ACCT_I, DT1.INT_GRUP_I, C.CALENDAR_DATE ORDER BY DT1.EFFT_D DESC NULLS LAST, DT1.REL_C DESC NULLS LAST) = 1) AS DT2
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
  SELECT
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
      OR "a"."expy_d" <> "b"."expy_d"
      OR "a"."pros_key_i" <> "b"."pros_key_i"
    )
    AND "a"."int_grup_i" = "b"."int_grup_i"
    AND "a"."join_from_d" <= "b"."join_to_d"
    AND "a"."join_to_d" >= "b"."join_from_d"
  JOIN "%%vtech%%"."calendar" AS "c"
    ON "a"."join_from_d" <= "c"."calendar_date"
    AND "a"."join_to_d" >= "c"."calendar_date"
    AND "c"."calendar_date" <= ADD_MONTHS(CURRENT_DATE, 1)
    AND "c"."calendar_date" >= ADD_MONTHS((
      CURRENT_DATE - DATE_PART(DAY, CURRENT_DATE) + 1
    ), -39)
  QUALIFY
    ROW_NUMBER() OVER (
      PARTITION BY "a"."acct_i", "a"."int_grup_i", "c"."calendar_date"
      ORDER BY "a"."efft_d" DESC NULLS LAST, "a"."rel_c" DESC NULLS LAST
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
  ROW_NUMBER() OVER (
    PARTITION BY "dt2"."int_grup_i", "dt2"."acct_i"
    ORDER BY "dt2"."perd_d" NULLS FIRST, "dt2"."rel_c" NULLS FIRST
  ) AS "grup_n",
  "dt2"."row_secu_accs_c" AS "row_secu_accs_c",
  0 AS "pros_key_i"
FROM "dt2" AS "dt2"
```

#### 📊 SQL Metadata:
- **Tables**: CALENDAR, DERV_PRTF_ACCT_INT_GRUP_PSST, DERV_PRTF_ACCT_PSST
- **Columns**: ACCT_I, CALENDAR_DATE, EFFT_D, EXPY_D, INT_GRUP_I, JOIN_FROM_D, JOIN_TO_D, PERD_D, PROS_KEY_I, REL_C, ROW_SECU_ACCS_C, SRCE_SYST_C, VALD_FROM_D, VALD_TO_D
- **Functions**: (CURRENT_DATE - EXTRACT(DAY FROM CURRENT_DATE) + 1), A.ACCT_I = B.ACCT_I, A.ACCT_I = B.ACCT_I AND A.INT_GRUP_I = B.INT_GRUP_I, A.ACCT_I = B.ACCT_I AND A.INT_GRUP_I = B.INT_GRUP_I AND A.JOIN_TO_D >= B.JOIN_FROM_D, A.ACCT_I = B.ACCT_I AND A.INT_GRUP_I = B.INT_GRUP_I AND A.JOIN_TO_D >= B.JOIN_FROM_D AND A.JOIN_FROM_D <= B.JOIN_TO_D, A.EFFT_D <> B.EFFT_D, A.EFFT_D <> B.EFFT_D OR A.EXPY_D <> B.EXPY_D, C.CALENDAR_DATE BETWEEN DT1.JOIN_FROM_D AND DT1.JOIN_TO_D, CURRENT_DATE, DAY, None
- **Window_Functions**: None

### SQL Block 5 (Lines 164-165)
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

### SQL Block 6 (Lines 169-223)
- **Complexity Score**: 240
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
Insert into %%STARDATADB%%.DERV_PRTF_ACCT_HIST_PSST
Select Distinct
   DT2.INT_GRUP_I
  ,DT2.ACCT_I
  ,DT2.REL_C
  ,MIN(DT2.PERD_D) Over ( Partition By DT2.ACCT_I, DT2.INT_GRUP_I, DT2.GRUP_N ) as JOIN_FROM_D
  ,MAX(DT2.PERD_D) Over ( Partition By DT2.ACCT_I, DT2.INT_GRUP_I, DT2.GRUP_N ) as JOIN_TO_D
  ,DT2.VALD_FROM_D
  ,DT2.VALD_TO_D
  ,DT2.EFFT_D
  ,DT2.EXPY_D
  ,DT2.SRCE_SYST_C
  ,DT2.ROW_SECU_ACCS_C
From
  (
  Select Distinct
     C.INT_GRUP_I
    ,C.ACCT_I
    ,C.REL_C
    ,C.VALD_FROM_D
    ,C.VALD_TO_D
    ,C.EFFT_D
    ,C.EXPY_D
    ,C.SRCE_SYST_C
    ,C.ROW_SECU_ACCS_C
    ,C.PERD_D
    ,MAX(Coalesce( DT1.ROW_N, 0 )) Over (Partition By C.ACCT_I, C.INT_GRUP_I, C.PERD_D) as GRUP_N     
  From
    %%VTECH%%.DERV_PRTF_ACCT_INT_GRUP_PSST C
    Left Join (
      -- Detect the change in non-key values between rows
      Select
         A.INT_GRUP_I
        ,A.ACCT_I
        ,A.PERD_D
        ,A.REL_C
        ,A.ROW_N
        ,A.EFFT_D
      From
        %%VTECH%%.DERV_PRTF_ACCT_INT_GRUP_PSST A
        Inner Join %%VTECH%%.DERV_PRTF_ACCT_INT_GRUP_PSST B
        On A.ACCT_I = B.ACCT_I
        And A.INT_GRUP_I = B.INT_GRUP_I
        And A.ROW_N = B.ROW_N + 1
        And A.EFFT_D <> B.EFFT_D
    ) DT1
    On C.INT_GRUP_I = DT1.INT_GRUP_I
    And C.ACCT_I = DT1.ACCT_I
    And C.REL_C = DT1.REL_C
    And C.EFFT_D = DT1.EFFT_D
    And C.ROW_N >= DT1.ROW_N
    And DT1.PERD_D <= C.PERD_D    

  ) DT2
;
```

#### ❄️ Converted Snowflake SQL:
```sql
INSERT INTO %%STARDATADB%%.DERV_PRTF_ACCT_HIST_PSST SELECT DISTINCT DT2.INT_GRUP_I, DT2.ACCT_I, DT2.REL_C, MIN(DT2.PERD_D) OVER (PARTITION BY DT2.ACCT_I, DT2.INT_GRUP_I, DT2.GRUP_N) AS JOIN_FROM_D, MAX(DT2.PERD_D) OVER (PARTITION BY DT2.ACCT_I, DT2.INT_GRUP_I, DT2.GRUP_N) AS JOIN_TO_D, DT2.VALD_FROM_D, DT2.VALD_TO_D, DT2.EFFT_D, DT2.EXPY_D, DT2.SRCE_SYST_C, DT2.ROW_SECU_ACCS_C FROM (SELECT DISTINCT C.INT_GRUP_I, C.ACCT_I, C.REL_C, C.VALD_FROM_D, C.VALD_TO_D, C.EFFT_D, C.EXPY_D, C.SRCE_SYST_C, C.ROW_SECU_ACCS_C, C.PERD_D, MAX(COALESCE(DT1.ROW_N, 0)) OVER (PARTITION BY C.ACCT_I, C.INT_GRUP_I, C.PERD_D) AS GRUP_N FROM %%VTECH%%.DERV_PRTF_ACCT_INT_GRUP_PSST AS C LEFT JOIN (SELECT A.INT_GRUP_I, A.ACCT_I, A.PERD_D, A.REL_C, A.ROW_N, A.EFFT_D FROM %%VTECH%%.DERV_PRTF_ACCT_INT_GRUP_PSST AS A INNER JOIN %%VTECH%%.DERV_PRTF_ACCT_INT_GRUP_PSST AS B ON A.ACCT_I = B.ACCT_I AND A.INT_GRUP_I = B.INT_GRUP_I AND A.ROW_N = B.ROW_N + 1 AND A.EFFT_D <> B.EFFT_D) AS DT1 ON C.INT_GRUP_I = DT1.INT_GRUP_I AND C.ACCT_I = DT1.ACCT_I AND C.REL_C = DT1.REL_C AND C.EFFT_D = DT1.EFFT_D AND C.ROW_N >= DT1.ROW_N AND DT1.PERD_D <= C.PERD_D) AS DT2
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
INSERT INTO "%%stardatadb%%"."derv_prtf_acct_hist_psst"
WITH "dt2" AS (
  SELECT DISTINCT
    "c"."int_grup_i" AS "int_grup_i",
    "c"."acct_i" AS "acct_i",
    "c"."rel_c" AS "rel_c",
    "c"."vald_from_d" AS "vald_from_d",
    "c"."vald_to_d" AS "vald_to_d",
    "c"."efft_d" AS "efft_d",
    "c"."expy_d" AS "expy_d",
    "c"."srce_syst_c" AS "srce_syst_c",
    "c"."row_secu_accs_c" AS "row_secu_accs_c",
    "c"."perd_d" AS "perd_d",
    MAX(COALESCE("a"."row_n", 0)) OVER (PARTITION BY "c"."acct_i", "c"."int_grup_i", "c"."perd_d") AS "grup_n"
  FROM "%%vtech%%"."derv_prtf_acct_int_grup_psst" AS "c"
  LEFT JOIN "%%vtech%%"."derv_prtf_acct_int_grup_psst" AS "a"
    ON "a"."acct_i" = "c"."acct_i"
    AND "a"."efft_d" = "c"."efft_d"
    AND "a"."int_grup_i" = "c"."int_grup_i"
    AND "a"."perd_d" <= "c"."perd_d"
    AND "a"."rel_c" = "c"."rel_c"
    AND "a"."row_n" <= "c"."row_n"
  JOIN "%%vtech%%"."derv_prtf_acct_int_grup_psst" AS "b"
    ON "a"."acct_i" = "b"."acct_i"
    AND "a"."efft_d" <> "b"."efft_d"
    AND "a"."int_grup_i" = "b"."int_grup_i"
    AND "a"."row_n" = "b"."row_n" + 1
)
SELECT DISTINCT
  "dt2"."int_grup_i" AS "int_grup_i",
  "dt2"."acct_i" AS "acct_i",
  "dt2"."rel_c" AS "rel_c",
  MIN("dt2"."perd_d") OVER (PARTITION BY "dt2"."acct_i", "dt2"."int_grup_i", "dt2"."grup_n") AS "join_from_d",
  MAX("dt2"."perd_d") OVER (PARTITION BY "dt2"."acct_i", "dt2"."int_grup_i", "dt2"."grup_n") AS "join_to_d",
  "dt2"."vald_from_d" AS "vald_from_d",
  "dt2"."vald_to_d" AS "vald_to_d",
  "dt2"."efft_d" AS "efft_d",
  "dt2"."expy_d" AS "expy_d",
  "dt2"."srce_syst_c" AS "srce_syst_c",
  "dt2"."row_secu_accs_c" AS "row_secu_accs_c"
FROM "dt2" AS "dt2"
```

#### 📊 SQL Metadata:
- **Tables**: DERV_PRTF_ACCT_HIST_PSST, DERV_PRTF_ACCT_INT_GRUP_PSST
- **Columns**: ACCT_I, EFFT_D, EXPY_D, GRUP_N, INT_GRUP_I, PERD_D, REL_C, ROW_N, ROW_SECU_ACCS_C, SRCE_SYST_C, VALD_FROM_D, VALD_TO_D
- **Functions**: A.ACCT_I = B.ACCT_I, A.ACCT_I = B.ACCT_I AND A.INT_GRUP_I = B.INT_GRUP_I, A.ACCT_I = B.ACCT_I AND A.INT_GRUP_I = B.INT_GRUP_I AND A.ROW_N = B.ROW_N + 1, C.INT_GRUP_I = DT1.INT_GRUP_I, C.INT_GRUP_I = DT1.INT_GRUP_I AND C.ACCT_I = DT1.ACCT_I, C.INT_GRUP_I = DT1.INT_GRUP_I AND C.ACCT_I = DT1.ACCT_I AND C.REL_C = DT1.REL_C, C.INT_GRUP_I = DT1.INT_GRUP_I AND C.ACCT_I = DT1.ACCT_I AND C.REL_C = DT1.REL_C AND C.EFFT_D = DT1.EFFT_D, C.INT_GRUP_I = DT1.INT_GRUP_I AND C.ACCT_I = DT1.ACCT_I AND C.REL_C = DT1.REL_C AND C.EFFT_D = DT1.EFFT_D AND C.ROW_N >= DT1.ROW_N, COALESCE(DT1.ROW_N, 0), DT1.ROW_N, DT2.PERD_D
- **Window_Functions**: COALESCE(DT1.ROW_N, 0), DT2.PERD_D

### SQL Block 7 (Lines 236-247)
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
  And A.INT_GRUP_I = B.INT_GRUP_I
  --And (A.JOIN_FROM_D,A.JOIN_TO_D) overlaps (B.JOIN_FROM_D,B.JOIN_TO_D)   
  And A.JOIN_TO_D >= B.JOIN_FROM_D
  And A.JOIN_FROM_D <= B.JOIN_TO_D  
;
```

#### ❄️ Converted Snowflake SQL:
```sql
DELETE A FROM %%STARDATADB%%.DERV_PRTF_ACCT_PSST AS A, %%VTECH%%.DERV_PRTF_ACCT_HIST_PSST AS B WHERE A.ACCT_I = B.ACCT_I AND A.INT_GRUP_I = B.INT_GRUP_I AND A.JOIN_TO_D >= B.JOIN_FROM_D AND A.JOIN_FROM_D <= B.JOIN_TO_D
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
  AND "a"."int_grup_i" = "b"."int_grup_i"
  AND "a"."join_from_d" <= "b"."join_to_d"
  AND "a"."join_to_d" >= "b"."join_from_d"
```

#### 📊 SQL Metadata:
- **Tables**: A, DERV_PRTF_ACCT_HIST_PSST, DERV_PRTF_ACCT_PSST
- **Columns**: ACCT_I, INT_GRUP_I, JOIN_FROM_D, JOIN_TO_D
- **Functions**: A.ACCT_I = B.ACCT_I, A.ACCT_I = B.ACCT_I AND A.INT_GRUP_I = B.INT_GRUP_I, A.ACCT_I = B.ACCT_I AND A.INT_GRUP_I = B.INT_GRUP_I AND A.JOIN_TO_D >= B.JOIN_FROM_D

### SQL Block 8 (Lines 255-275)
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

*Generated by BTEQ Parser Service - 2025-08-20 12:39:19*
