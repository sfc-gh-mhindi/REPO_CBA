# prtf_tech_int_grup_enhc_psst.sql - BTEQ Analysis

## File Overview
- **File Name**: prtf_tech_int_grup_enhc_psst.sql
- **Analysis Status**: ✅ Success
- **Control Statements**: 15
- **SQL Blocks**: 6

## Control Flow Analysis

| Line | Type | Statement |
|------|------|-----------|
| 1 | RUN | `.RUN FILE=%%BTEQ_LOGON_SCRIPT%%` |
| 2 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 35 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 63 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 67 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 76 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 79 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 131 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 134 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 141 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 197 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 201 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 205 | LOGOFF | `.LOGOFF` |
| 207 | LABEL | `.LABEL EXITERR` |
| 209 | LOGOFF | `.LOGOFF` |

## SQL Blocks Analysis

### SQL Block 1 (Lines 33-34)
- **Complexity Score**: 4
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
Delete from %%STARDATADB%%.DERV_PRTF_INT_PSST
;
```

#### ❄️ Converted Snowflake SQL:
```sql
DELETE FROM %%STARDATADB%%.DERV_PRTF_INT_PSST
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
DELETE FROM "%%stardatadb%%"."derv_prtf_int_psst"
```

#### 📊 SQL Metadata:
- **Tables**: DERV_PRTF_INT_PSST

### SQL Block 2 (Lines 38-62)
- **Complexity Score**: 107
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
Insert into %%STARDATADB%%.DERV_PRTF_INT_PSST
Select
   A.INT_GRUP_I
  ,A.INT_GRUP_TYPE_C
  ,A.CRAT_D as JOIN_FROM_D
  ,A.VALD_TO_D as  JOIN_TO_D
  ,A.EFFT_D
  ,A.EXPY_D  
  ,A.PTCL_N
  ,A.REL_MNGE_I 
  ,A.CRAT_D as VALD_FROM_D
  ,A.VALD_TO_D  
  ,A.PROS_KEY_EFFT_I 
From
	%%VTECH%%.INT_GRUP A
		
	/* Use new History table */
	Inner Join %%VTECH%%.GRD_PRTF_TYPE_ENHC_HIST_PSST GPTE
	On GPTE.PRTF_TYPE_C = A.INT_GRUP_TYPE_C
	And (GPTE.VALD_FROM_D, GPTE.VALD_TO_D) Overlaps (A.CRAT_D, A.VALD_TO_D)

  And CHAR2HEXINT( UPPER(Coalesce(A.PTCL_N,'0') )) = CHAR2HEXINT( LOWER(Coalesce(A.PTCL_N,'0') ))

Group By 1,2,3,4,5,6,7,8,9,10,11
;
```

#### ❄️ Converted Snowflake SQL:
```sql
INSERT INTO %%STARDATADB%%.DERV_PRTF_INT_PSST SELECT A.INT_GRUP_I, A.INT_GRUP_TYPE_C, A.CRAT_D AS JOIN_FROM_D, A.VALD_TO_D AS JOIN_TO_D, A.EFFT_D, A.EXPY_D, A.PTCL_N, A.REL_MNGE_I, A.CRAT_D AS VALD_FROM_D, A.VALD_TO_D, A.PROS_KEY_EFFT_I FROM %%VTECH%%.INT_GRUP AS A INNER JOIN %%VTECH%%.GRD_PRTF_TYPE_ENHC_HIST_PSST AS GPTE ON GPTE.PRTF_TYPE_C = A.INT_GRUP_TYPE_C AND (GPTE.VALD_FROM_D, GPTE.VALD_TO_D) OVERLAPS (A.CRAT_D, A.VALD_TO_D) AND CHAR2HEXINT(UPPER(COALESCE(A.PTCL_N, '0'))) = CHAR2HEXINT(LOWER(COALESCE(A.PTCL_N, '0'))) GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
INSERT INTO "%%stardatadb%%"."derv_prtf_int_psst"
SELECT
  "a"."int_grup_i" AS "int_grup_i",
  "a"."int_grup_type_c" AS "int_grup_type_c",
  "a"."crat_d" AS "join_from_d",
  "a"."vald_to_d" AS "join_to_d",
  "a"."efft_d" AS "efft_d",
  "a"."expy_d" AS "expy_d",
  "a"."ptcl_n" AS "ptcl_n",
  "a"."rel_mnge_i" AS "rel_mnge_i",
  "a"."crat_d" AS "vald_from_d",
  "a"."vald_to_d" AS "vald_to_d",
  "a"."pros_key_efft_i" AS "pros_key_efft_i"
FROM "%%vtech%%"."int_grup" AS "a"
JOIN "%%vtech%%"."grd_prtf_type_enhc_hist_psst" AS "gpte"
  ON "a"."int_grup_type_c" = "gpte"."prtf_type_c"
  AND CHAR2HEXINT(LOWER(COALESCE("a"."ptcl_n", '0'))) = CHAR2HEXINT(UPPER(COALESCE("a"."ptcl_n", '0')))
  AND ("gpte"."vald_from_d", "gpte"."vald_to_d") OVERLAPS ("a"."crat_d", "a"."vald_to_d")
GROUP BY
  "a"."int_grup_i",
  "a"."int_grup_type_c",
  "a"."crat_d",
  "a"."vald_to_d",
  "a"."efft_d",
  "a"."expy_d",
  "a"."ptcl_n",
  "a"."rel_mnge_i",
  "a"."crat_d",
  "a"."vald_to_d",
  "a"."pros_key_efft_i"
```

#### 📊 SQL Metadata:
- **Tables**: DERV_PRTF_INT_PSST, GRD_PRTF_TYPE_ENHC_HIST_PSST, INT_GRUP
- **Columns**: CRAT_D, EFFT_D, EXPY_D, INT_GRUP_I, INT_GRUP_TYPE_C, PROS_KEY_EFFT_I, PRTF_TYPE_C, PTCL_N, REL_MNGE_I, VALD_FROM_D, VALD_TO_D
- **Functions**: A.PTCL_N, CHAR2HEXINT, COALESCE(A.PTCL_N, '0'), GPTE.PRTF_TYPE_C = A.INT_GRUP_TYPE_C, GPTE.PRTF_TYPE_C = A.INT_GRUP_TYPE_C AND (GPTE.VALD_FROM_D, GPTE.VALD_TO_D) OVERLAPS (A.CRAT_D, A.VALD_TO_D)

### SQL Block 3 (Lines 74-75)
- **Complexity Score**: 6
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
Delete from %%STARDATADB%%.DERV_PRTF_INT_GRUP_ENHC_PSST All
;
```

#### ❄️ Converted Snowflake SQL:
```sql
DELETE FROM %%STARDATADB%%.DERV_PRTF_INT_GRUP_ENHC_PSST AS All
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
DELETE FROM "%%stardatadb%%"."derv_prtf_int_grup_enhc_psst" AS "all"
```

#### 📊 SQL Metadata:
- **Tables**: DERV_PRTF_INT_GRUP_ENHC_PSST

### SQL Block 4 (Lines 82-130)
- **Complexity Score**: 254
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 5

#### 📝 Original Teradata SQL:
```sql
Insert into %%STARDATADB%%.DERV_PRTF_INT_GRUP_ENHC_PSST
Select
   DT2.INT_GRUP_I
  ,DT2.INT_GRUP_TYPE_C
  ,DT2.EFFT_D
  ,DT2.EXPY_D
  ,DT2.VALD_FROM_D
  ,DT2.VALD_TO_D
  ,DT2.PERD_D       
  ,DT2.PTCL_N
  ,DT2.REL_MNGE_I
  ,ROW_NUMBER() OVER (Partition By DT2.INT_GRUP_I Order by DT2.PERD_D )
  ,0 as PROS_KEY  
From
	(
				Select
					 A.INT_GRUP_I
					,A.INT_GRUP_TYPE_C
					,A.PTCL_N
					,C.CALENDAR_DATE as PERD_D
					,A.EFFT_D
					,A.EXPY_D
					,A.VALD_FROM_D
					,A.VALD_TO_D
					,A.REL_MNGE_I 
				From
          %%VTECH%%.DERV_PRTF_INT_PSST A
          Inner Join %%VTECH%%.DERV_PRTF_INT_PSST B
					On A.INT_GRUP_I = B.INT_GRUP_I
					And CHAR2HEXINT( UPPER(Coalesce(A.PTCL_N,'0') )) = CHAR2HEXINT( LOWER(Coalesce(A.PTCL_N,'0') ))
					And CHAR2HEXINT( UPPER(Coalesce(B.PTCL_N,'0') )) = CHAR2HEXINT( LOWER(Coalesce(B.PTCL_N,'0') ))
					And (A.JOIN_FROM_D,A.JOIN_TO_D) Overlaps (B.JOIN_FROM_D,B.JOIN_TO_D)   
          And (         -- Updated Wed 6/11
            A.JOIN_FROM_D <> B.JOIN_FROM_D
            Or A.JOIN_TO_D <> B.JOIN_TO_D
            Or A.EFFT_D <> B.EFFT_D
            Or A.EXPY_D <> B.EXPY_D
            Or A.PTCL_N <> B.PTCL_N
            Or A.INT_GRUP_TYPE_C <> B.INT_GRUP_TYPE_C
            Or A.REL_MNGE_I <> B.REL_MNGE_I
          )

					Inner Join %%VTECH%%.CALENDAR C
					On C.CALENDAR_DATE between A.JOIN_FROM_D and A.JOIN_TO_D
					And C.CALENDAR_DATE between ADD_MONTHS((Current_Date - EXTRACT(DAY from CURRENT_DATE) +1 ),-39) and ADD_MONTHS(Current_Date, 1)

		Qualify Row_Number() Over( Partition By A.INT_GRUP_I, C.CALENDAR_DATE Order BY A.EFFT_D Desc) = 1
	) DT2
;
```

#### ❄️ Converted Snowflake SQL:
```sql
INSERT INTO %%STARDATADB%%.DERV_PRTF_INT_GRUP_ENHC_PSST SELECT DT2.INT_GRUP_I, DT2.INT_GRUP_TYPE_C, DT2.EFFT_D, DT2.EXPY_D, DT2.VALD_FROM_D, DT2.VALD_TO_D, DT2.PERD_D, DT2.PTCL_N, DT2.REL_MNGE_I, ROW_NUMBER() OVER (PARTITION BY DT2.INT_GRUP_I ORDER BY DT2.PERD_D NULLS FIRST), 0 AS PROS_KEY FROM (SELECT A.INT_GRUP_I, A.INT_GRUP_TYPE_C, A.PTCL_N, C.CALENDAR_DATE AS PERD_D, A.EFFT_D, A.EXPY_D, A.VALD_FROM_D, A.VALD_TO_D, A.REL_MNGE_I FROM %%VTECH%%.DERV_PRTF_INT_PSST AS A INNER JOIN %%VTECH%%.DERV_PRTF_INT_PSST AS B ON A.INT_GRUP_I = B.INT_GRUP_I AND CHAR2HEXINT(UPPER(COALESCE(A.PTCL_N, '0'))) = CHAR2HEXINT(LOWER(COALESCE(A.PTCL_N, '0'))) AND CHAR2HEXINT(UPPER(COALESCE(B.PTCL_N, '0'))) = CHAR2HEXINT(LOWER(COALESCE(B.PTCL_N, '0'))) AND (A.JOIN_FROM_D, A.JOIN_TO_D) OVERLAPS (B.JOIN_FROM_D, B.JOIN_TO_D) AND (A.JOIN_FROM_D <> B.JOIN_FROM_D OR A.JOIN_TO_D <> B.JOIN_TO_D OR A.EFFT_D <> B.EFFT_D OR A.EXPY_D <> B.EXPY_D OR A.PTCL_N <> B.PTCL_N OR A.INT_GRUP_TYPE_C <> B.INT_GRUP_TYPE_C OR A.REL_MNGE_I <> B.REL_MNGE_I) INNER JOIN %%VTECH%%.CALENDAR AS C ON C.CALENDAR_DATE BETWEEN A.JOIN_FROM_D AND A.JOIN_TO_D AND C.CALENDAR_DATE BETWEEN ADD_MONTHS((CURRENT_DATE - DATE_PART(DAY, CURRENT_DATE) + 1), -39) AND ADD_MONTHS(CURRENT_DATE, 1) QUALIFY ROW_NUMBER() OVER (PARTITION BY A.INT_GRUP_I, C.CALENDAR_DATE ORDER BY A.EFFT_D DESC NULLS LAST) = 1) AS DT2
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
INSERT INTO "%%stardatadb%%"."derv_prtf_int_grup_enhc_psst"
WITH "dt2" AS (
  SELECT
    "a"."int_grup_i" AS "int_grup_i",
    "a"."int_grup_type_c" AS "int_grup_type_c",
    "a"."ptcl_n" AS "ptcl_n",
    "c"."calendar_date" AS "perd_d",
    "a"."efft_d" AS "efft_d",
    "a"."expy_d" AS "expy_d",
    "a"."vald_from_d" AS "vald_from_d",
    "a"."vald_to_d" AS "vald_to_d",
    "a"."rel_mnge_i" AS "rel_mnge_i"
  FROM "%%vtech%%"."derv_prtf_int_psst" AS "a"
  JOIN "%%vtech%%"."derv_prtf_int_psst" AS "b"
    ON (
      "a"."efft_d" <> "b"."efft_d"
      OR "a"."expy_d" <> "b"."expy_d"
      OR "a"."int_grup_type_c" <> "b"."int_grup_type_c"
      OR "a"."join_from_d" <> "b"."join_from_d"
      OR "a"."join_to_d" <> "b"."join_to_d"
      OR "a"."ptcl_n" <> "b"."ptcl_n"
      OR "a"."rel_mnge_i" <> "b"."rel_mnge_i"
    )
    AND "a"."int_grup_i" = "b"."int_grup_i"
    AND CHAR2HEXINT(LOWER(COALESCE("a"."ptcl_n", '0'))) = CHAR2HEXINT(UPPER(COALESCE("a"."ptcl_n", '0')))
    AND CHAR2HEXINT(LOWER(COALESCE("b"."ptcl_n", '0'))) = CHAR2HEXINT(UPPER(COALESCE("b"."ptcl_n", '0')))
    AND ("a"."join_from_d", "a"."join_to_d") OVERLAPS ("b"."join_from_d", "b"."join_to_d")
  JOIN "%%vtech%%"."calendar" AS "c"
    ON "a"."join_from_d" <= "c"."calendar_date"
    AND "a"."join_to_d" >= "c"."calendar_date"
    AND "c"."calendar_date" <= ADD_MONTHS(CURRENT_DATE, 1)
    AND "c"."calendar_date" >= ADD_MONTHS((
      CURRENT_DATE - DATE_PART(DAY, CURRENT_DATE) + 1
    ), -39)
  QUALIFY
    ROW_NUMBER() OVER (
      PARTITION BY "a"."int_grup_i", "c"."calendar_date"
      ORDER BY "a"."efft_d" DESC NULLS LAST
    ) = 1
)
SELECT
  "dt2"."int_grup_i" AS "int_grup_i",
  "dt2"."int_grup_type_c" AS "int_grup_type_c",
  "dt2"."efft_d" AS "efft_d",
  "dt2"."expy_d" AS "expy_d",
  "dt2"."vald_from_d" AS "vald_from_d",
  "dt2"."vald_to_d" AS "vald_to_d",
  "dt2"."perd_d" AS "perd_d",
  "dt2"."ptcl_n" AS "ptcl_n",
  "dt2"."rel_mnge_i" AS "rel_mnge_i",
  ROW_NUMBER() OVER (PARTITION BY "dt2"."int_grup_i" ORDER BY "dt2"."perd_d" NULLS FIRST) AS "_col_9",
  0 AS "pros_key"
FROM "dt2" AS "dt2"
```

#### 📊 SQL Metadata:
- **Tables**: CALENDAR, DERV_PRTF_INT_GRUP_ENHC_PSST, DERV_PRTF_INT_PSST
- **Columns**: CALENDAR_DATE, EFFT_D, EXPY_D, INT_GRUP_I, INT_GRUP_TYPE_C, JOIN_FROM_D, JOIN_TO_D, PERD_D, PTCL_N, REL_MNGE_I, VALD_FROM_D, VALD_TO_D
- **Functions**: (CURRENT_DATE - EXTRACT(DAY FROM CURRENT_DATE) + 1), A.INT_GRUP_I = B.INT_GRUP_I, A.INT_GRUP_I = B.INT_GRUP_I AND CHAR2HEXINT(UPPER(COALESCE(A.PTCL_N, '0'))) = CHAR2HEXINT(LOWER(COALESCE(A.PTCL_N, '0'))), A.INT_GRUP_I = B.INT_GRUP_I AND CHAR2HEXINT(UPPER(COALESCE(A.PTCL_N, '0'))) = CHAR2HEXINT(LOWER(COALESCE(A.PTCL_N, '0'))) AND CHAR2HEXINT(UPPER(COALESCE(B.PTCL_N, '0'))) = CHAR2HEXINT(LOWER(COALESCE(B.PTCL_N, '0'))), A.INT_GRUP_I = B.INT_GRUP_I AND CHAR2HEXINT(UPPER(COALESCE(A.PTCL_N, '0'))) = CHAR2HEXINT(LOWER(COALESCE(A.PTCL_N, '0'))) AND CHAR2HEXINT(UPPER(COALESCE(B.PTCL_N, '0'))) = CHAR2HEXINT(LOWER(COALESCE(B.PTCL_N, '0'))) AND (A.JOIN_FROM_D, A.JOIN_TO_D) OVERLAPS (B.JOIN_FROM_D, B.JOIN_TO_D), A.JOIN_FROM_D <> B.JOIN_FROM_D, A.JOIN_FROM_D <> B.JOIN_FROM_D OR A.JOIN_TO_D <> B.JOIN_TO_D, A.JOIN_FROM_D <> B.JOIN_FROM_D OR A.JOIN_TO_D <> B.JOIN_TO_D OR A.EFFT_D <> B.EFFT_D, A.JOIN_FROM_D <> B.JOIN_FROM_D OR A.JOIN_TO_D <> B.JOIN_TO_D OR A.EFFT_D <> B.EFFT_D OR A.EXPY_D <> B.EXPY_D, A.JOIN_FROM_D <> B.JOIN_FROM_D OR A.JOIN_TO_D <> B.JOIN_TO_D OR A.EFFT_D <> B.EFFT_D OR A.EXPY_D <> B.EXPY_D OR A.PTCL_N <> B.PTCL_N, A.JOIN_FROM_D <> B.JOIN_FROM_D OR A.JOIN_TO_D <> B.JOIN_TO_D OR A.EFFT_D <> B.EFFT_D OR A.EXPY_D <> B.EXPY_D OR A.PTCL_N <> B.PTCL_N OR A.INT_GRUP_TYPE_C <> B.INT_GRUP_TYPE_C, A.PTCL_N, B.PTCL_N, C.CALENDAR_DATE BETWEEN A.JOIN_FROM_D AND A.JOIN_TO_D, CHAR2HEXINT, COALESCE(A.PTCL_N, '0'), COALESCE(B.PTCL_N, '0'), CURRENT_DATE, DAY, None
- **Window_Functions**: None

### SQL Block 5 (Lines 139-140)
- **Complexity Score**: 6
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
Delete from %%STARDATADB%%.DERV_PRTF_INT_HIST_PSST All
;
```

#### ❄️ Converted Snowflake SQL:
```sql
DELETE FROM %%STARDATADB%%.DERV_PRTF_INT_HIST_PSST AS All
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
DELETE FROM "%%stardatadb%%"."derv_prtf_int_hist_psst" AS "all"
```

#### 📊 SQL Metadata:
- **Tables**: DERV_PRTF_INT_HIST_PSST

### SQL Block 6 (Lines 143-196)
- **Complexity Score**: 228
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
Insert into %%STARDATADB%%.DERV_PRTF_INT_HIST_PSST
Select Distinct
	 DT2.INT_GRUP_I
	,DT2.INT_GRUP_TYPE_C
	,DT2.EFFT_D
	,DT2.EXPY_D
	,DT2.VALD_FROM_D
	,DT2.VALD_TO_D
  ,MIN(DT2.PERD_D) Over ( Partition By DT2.INT_GRUP_I, DT2.GRUP_N ) as JOIN_FROM_D
  ,MAX(DT2.PERD_D) Over ( Partition By DT2.INT_GRUP_I, DT2.GRUP_N ) as JOIN_TO_D
	,DT2.PTCL_N
	,DT2.REL_MNGE_I
From
  (
  Select
		 C.INT_GRUP_I
		,C.INT_GRUP_TYPE_C
		,C.EFFT_D
		,C.EXPY_D
		,C.VALD_FROM_D
		,C.VALD_TO_D
		,C.PERD_D       
		,C.PTCL_N
		,C.REL_MNGE_I
    ,MAX(Coalesce( DT1.ROW_N, 0 )) Over (Partition By C.INT_GRUP_I, C.PTCL_N, C.REL_MNGE_I, C.PERD_D) as GRUP_N 	
  From
    %%VTECH%%.DERV_PRTF_INT_GRUP_ENHC_PSST C
    Left Join (
      -- Detect the change in non-key values between rows
      Select
         A.INT_GRUP_I
        ,A.PTCL_N
        ,A.REL_MNGE_I
        ,A.PERD_D
        ,A.ROW_N
      From
        %%VTECH%%.DERV_PRTF_INT_GRUP_ENHC_PSST A
        Inner Join %%VTECH%%.DERV_PRTF_INT_GRUP_ENHC_PSST B
        On A.INT_GRUP_I = B.INT_GRUP_I
        And A.ROW_N = B.ROW_N + 1
        And (
					A.INT_GRUP_TYPE_C <> B.INT_GRUP_TYPE_C
					Or A.PTCL_N <> B.PTCL_N
					Or A.REL_MNGE_I <> B.REL_MNGE_I
				)
    ) DT1
    On C.INT_GRUP_I = DT1.INT_GRUP_I
		And C.PTCL_N = DT1.PTCL_N
		And C.REL_MNGE_I = DT1.REL_MNGE_I
    And C.ROW_N >= DT1.ROW_N
    And DT1.PERD_D <= C.PERD_D 

  ) DT2
;
```

#### ❄️ Converted Snowflake SQL:
```sql
INSERT INTO %%STARDATADB%%.DERV_PRTF_INT_HIST_PSST SELECT DISTINCT DT2.INT_GRUP_I, DT2.INT_GRUP_TYPE_C, DT2.EFFT_D, DT2.EXPY_D, DT2.VALD_FROM_D, DT2.VALD_TO_D, MIN(DT2.PERD_D) OVER (PARTITION BY DT2.INT_GRUP_I, DT2.GRUP_N) AS JOIN_FROM_D, MAX(DT2.PERD_D) OVER (PARTITION BY DT2.INT_GRUP_I, DT2.GRUP_N) AS JOIN_TO_D, DT2.PTCL_N, DT2.REL_MNGE_I FROM (SELECT C.INT_GRUP_I, C.INT_GRUP_TYPE_C, C.EFFT_D, C.EXPY_D, C.VALD_FROM_D, C.VALD_TO_D, C.PERD_D, C.PTCL_N, C.REL_MNGE_I, MAX(COALESCE(DT1.ROW_N, 0)) OVER (PARTITION BY C.INT_GRUP_I, C.PTCL_N, C.REL_MNGE_I, C.PERD_D) AS GRUP_N FROM %%VTECH%%.DERV_PRTF_INT_GRUP_ENHC_PSST AS C LEFT JOIN (SELECT A.INT_GRUP_I, A.PTCL_N, A.REL_MNGE_I, A.PERD_D, A.ROW_N FROM %%VTECH%%.DERV_PRTF_INT_GRUP_ENHC_PSST AS A INNER JOIN %%VTECH%%.DERV_PRTF_INT_GRUP_ENHC_PSST AS B ON A.INT_GRUP_I = B.INT_GRUP_I AND A.ROW_N = B.ROW_N + 1 AND (A.INT_GRUP_TYPE_C <> B.INT_GRUP_TYPE_C OR A.PTCL_N <> B.PTCL_N OR A.REL_MNGE_I <> B.REL_MNGE_I)) AS DT1 ON C.INT_GRUP_I = DT1.INT_GRUP_I AND C.PTCL_N = DT1.PTCL_N AND C.REL_MNGE_I = DT1.REL_MNGE_I AND C.ROW_N >= DT1.ROW_N AND DT1.PERD_D <= C.PERD_D) AS DT2
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
INSERT INTO "%%stardatadb%%"."derv_prtf_int_hist_psst"
WITH "dt2" AS (
  SELECT
    "c"."int_grup_i" AS "int_grup_i",
    "c"."int_grup_type_c" AS "int_grup_type_c",
    "c"."efft_d" AS "efft_d",
    "c"."expy_d" AS "expy_d",
    "c"."vald_from_d" AS "vald_from_d",
    "c"."vald_to_d" AS "vald_to_d",
    "c"."perd_d" AS "perd_d",
    "c"."ptcl_n" AS "ptcl_n",
    "c"."rel_mnge_i" AS "rel_mnge_i",
    MAX(COALESCE("a"."row_n", 0)) OVER (PARTITION BY "c"."int_grup_i", "c"."ptcl_n", "c"."rel_mnge_i", "c"."perd_d") AS "grup_n"
  FROM "%%vtech%%"."derv_prtf_int_grup_enhc_psst" AS "c"
  LEFT JOIN "%%vtech%%"."derv_prtf_int_grup_enhc_psst" AS "a"
    ON "a"."int_grup_i" = "c"."int_grup_i"
    AND "a"."perd_d" <= "c"."perd_d"
    AND "a"."ptcl_n" = "c"."ptcl_n"
    AND "a"."rel_mnge_i" = "c"."rel_mnge_i"
    AND "a"."row_n" <= "c"."row_n"
  JOIN "%%vtech%%"."derv_prtf_int_grup_enhc_psst" AS "b"
    ON "a"."int_grup_i" = "b"."int_grup_i"
    AND (
      "a"."int_grup_type_c" <> "b"."int_grup_type_c"
      OR "a"."ptcl_n" <> "b"."ptcl_n"
      OR "a"."rel_mnge_i" <> "b"."rel_mnge_i"
    )
    AND "a"."row_n" = "b"."row_n" + 1
)
SELECT DISTINCT
  "dt2"."int_grup_i" AS "int_grup_i",
  "dt2"."int_grup_type_c" AS "int_grup_type_c",
  "dt2"."efft_d" AS "efft_d",
  "dt2"."expy_d" AS "expy_d",
  "dt2"."vald_from_d" AS "vald_from_d",
  "dt2"."vald_to_d" AS "vald_to_d",
  MIN("dt2"."perd_d") OVER (PARTITION BY "dt2"."int_grup_i", "dt2"."grup_n") AS "join_from_d",
  MAX("dt2"."perd_d") OVER (PARTITION BY "dt2"."int_grup_i", "dt2"."grup_n") AS "join_to_d",
  "dt2"."ptcl_n" AS "ptcl_n",
  "dt2"."rel_mnge_i" AS "rel_mnge_i"
FROM "dt2" AS "dt2"
```

#### 📊 SQL Metadata:
- **Tables**: DERV_PRTF_INT_GRUP_ENHC_PSST, DERV_PRTF_INT_HIST_PSST
- **Columns**: EFFT_D, EXPY_D, GRUP_N, INT_GRUP_I, INT_GRUP_TYPE_C, PERD_D, PTCL_N, REL_MNGE_I, ROW_N, VALD_FROM_D, VALD_TO_D
- **Functions**: A.INT_GRUP_I = B.INT_GRUP_I, A.INT_GRUP_I = B.INT_GRUP_I AND A.ROW_N = B.ROW_N + 1, A.INT_GRUP_TYPE_C <> B.INT_GRUP_TYPE_C, A.INT_GRUP_TYPE_C <> B.INT_GRUP_TYPE_C OR A.PTCL_N <> B.PTCL_N, C.INT_GRUP_I = DT1.INT_GRUP_I, C.INT_GRUP_I = DT1.INT_GRUP_I AND C.PTCL_N = DT1.PTCL_N, C.INT_GRUP_I = DT1.INT_GRUP_I AND C.PTCL_N = DT1.PTCL_N AND C.REL_MNGE_I = DT1.REL_MNGE_I, C.INT_GRUP_I = DT1.INT_GRUP_I AND C.PTCL_N = DT1.PTCL_N AND C.REL_MNGE_I = DT1.REL_MNGE_I AND C.ROW_N >= DT1.ROW_N, COALESCE(DT1.ROW_N, 0), DT1.ROW_N, DT2.PERD_D
- **Window_Functions**: COALESCE(DT1.ROW_N, 0), DT2.PERD_D

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
