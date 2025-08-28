# prtf_tech_paty_psst.sql - BTEQ Analysis

## File Overview
- **File Name**: prtf_tech_paty_psst.sql
- **Analysis Status**: ✅ Success
- **Control Statements**: 12
- **SQL Blocks**: 6

## Control Flow Analysis

| Line | Type | Statement |
|------|------|-----------|
| 1 | RUN | `.RUN FILE=%%BTEQ_LOGON_SCRIPT%%` |
| 2 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 29 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 100 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 110 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 170 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 187 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 215 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 219 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 223 | LOGOFF | `.LOGOFF` |
| 225 | LABEL | `.LABEL EXITERR` |
| 227 | LOGOFF | `.LOGOFF` |

## SQL Blocks Analysis

### SQL Block 1 (Lines 27-28)
- **Complexity Score**: 4
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
Delete From %%STARDATADB%%.DERV_PRTF_PATY_INT_GRUP_PSST
;
```

#### ❄️ Converted Snowflake SQL:
```sql
DELETE FROM %%STARDATADB%%.DERV_PRTF_PATY_INT_GRUP_PSST
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
DELETE FROM "%%stardatadb%%"."derv_prtf_paty_int_grup_psst"
```

#### 📊 SQL Metadata:
- **Tables**: DERV_PRTF_PATY_INT_GRUP_PSST

### SQL Block 2 (Lines 31-99)
- **Complexity Score**: 260
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 5

#### 📝 Original Teradata SQL:
```sql
Insert into %%STARDATADB%%.DERV_PRTF_PATY_INT_GRUP_PSST
Select
	 DT2.INT_GRUP_I                    
	,DT2.PATY_I      
	,DT2.EFFT_D                        
	,DT2.EXPY_D                        
	,DT2.VALD_FROM_D                   
	,DT2.VALD_TO_D     
	,DT2.PERD_D                
	,DT2.REL_C                         
	,DT2.SRCE_SYST_C                   
	,ROW_NUMBER() OVER (Partition By DT2.PATY_I, DT2.REL_C  Order by DT2.PERD_D ) as GRUP_N
	,DT2.ROW_SECU_ACCS_C               
	,DT2.PROS_KEY_I   	
From
	(
		Select
				 DT1.INT_GRUP_I                    
				,DT1.PATY_I      
				,C.CALENDAR_DATE as PERD_D                                    
				,DT1.EFFT_D                        
				,DT1.EXPY_D                        
				,DT1.VALD_FROM_D                   
				,DT1.VALD_TO_D                     
				,DT1.REL_C                         
				,DT1.SRCE_SYST_C                   
				,DT1.ROW_SECU_ACCS_C               
				,DT1.PROS_KEY_I   
		From
			(
				Select
           A.INT_GRUP_I                    
          ,A.PATY_I                        
          ,A.JOIN_FROM_D
          ,A.JOIN_TO_D
          ,A.VALD_FROM_D
          ,A.VALD_TO_D
          ,A.EFFT_D                        
          ,A.EXPY_D                                         
          ,A.REL_C                         
          ,A.SRCE_SYST_C                   
          ,A.ROW_SECU_ACCS_C               
          ,A.PROS_KEY_I                    
				From
					%%VTECH%%.DERV_PRTF_PATY_PSST A
					Inner Join %%VTECH%%.DERV_PRTF_PATY_PSST B
					On A.PATY_I = B.PATY_I
					and A.REL_C = B.REL_C
--					And A.INT_GRUP_I <> B.INT_GRUP_I
					And (A.JOIN_FROM_D,A.JOIN_TO_D) Overlaps (B.JOIN_FROM_D, B.JOIN_TO_D)
          And (
            A.JOIN_FROM_D <> B.JOIN_FROM_D
            Or A.JOIN_TO_D <> B.JOIN_TO_D

            -- New record
            Or A.INT_GRUP_I <> B.INT_GRUP_I
          )			
					
			) DT1

			Inner Join %%VTECH%%.CALENDAR C
			--On C.CALENDAR_DATE between DT1.VALD_FROM_D and DT1.VALD_TO_D
			On C.CALENDAR_DATE between DT1.JOIN_FROM_D and DT1.JOIN_TO_D
			And C.CALENDAR_DATE between ADD_MONTHS((Current_Date - EXTRACT(DAY from CURRENT_DATE) +1 ),-39) and ADD_MONTHS(Current_Date, 1)

		Qualify Row_Number() Over( Partition By DT1.PATY_I, DT1.REL_C, C.CALENDAR_DATE Order BY DT1.EFFT_D Desc, DT1.INT_GRUP_I Desc) = 1

	) DT2
;
```

#### ❄️ Converted Snowflake SQL:
```sql
INSERT INTO %%STARDATADB%%.DERV_PRTF_PATY_INT_GRUP_PSST SELECT DT2.INT_GRUP_I, DT2.PATY_I, DT2.EFFT_D, DT2.EXPY_D, DT2.VALD_FROM_D, DT2.VALD_TO_D, DT2.PERD_D, DT2.REL_C, DT2.SRCE_SYST_C, ROW_NUMBER() OVER (PARTITION BY DT2.PATY_I, DT2.REL_C ORDER BY DT2.PERD_D NULLS FIRST) AS GRUP_N, DT2.ROW_SECU_ACCS_C, DT2.PROS_KEY_I FROM (SELECT DT1.INT_GRUP_I, DT1.PATY_I, C.CALENDAR_DATE AS PERD_D, DT1.EFFT_D, DT1.EXPY_D, DT1.VALD_FROM_D, DT1.VALD_TO_D, DT1.REL_C, DT1.SRCE_SYST_C, DT1.ROW_SECU_ACCS_C, DT1.PROS_KEY_I FROM (SELECT A.INT_GRUP_I, A.PATY_I, A.JOIN_FROM_D, A.JOIN_TO_D, A.VALD_FROM_D, A.VALD_TO_D, A.EFFT_D, A.EXPY_D, A.REL_C, A.SRCE_SYST_C, A.ROW_SECU_ACCS_C, A.PROS_KEY_I FROM %%VTECH%%.DERV_PRTF_PATY_PSST AS A INNER JOIN %%VTECH%%.DERV_PRTF_PATY_PSST AS B ON A.PATY_I = B.PATY_I AND A.REL_C = B.REL_C AND (A.JOIN_FROM_D, A.JOIN_TO_D) OVERLAPS (B.JOIN_FROM_D, B.JOIN_TO_D) AND (A.JOIN_FROM_D <> B.JOIN_FROM_D OR A.JOIN_TO_D <> B.JOIN_TO_D OR A.INT_GRUP_I <> B.INT_GRUP_I)) AS DT1 INNER JOIN %%VTECH%%.CALENDAR AS C ON C.CALENDAR_DATE BETWEEN DT1.JOIN_FROM_D AND DT1.JOIN_TO_D AND C.CALENDAR_DATE BETWEEN ADD_MONTHS((CURRENT_DATE - DATE_PART(DAY, CURRENT_DATE) + 1), -39) AND ADD_MONTHS(CURRENT_DATE, 1) QUALIFY ROW_NUMBER() OVER (PARTITION BY DT1.PATY_I, DT1.REL_C, C.CALENDAR_DATE ORDER BY DT1.EFFT_D DESC NULLS LAST, DT1.INT_GRUP_I DESC NULLS LAST) = 1) AS DT2
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
INSERT INTO "%%stardatadb%%"."derv_prtf_paty_int_grup_psst"
WITH "dt2" AS (
  SELECT
    "a"."int_grup_i" AS "int_grup_i",
    "a"."paty_i" AS "paty_i",
    "c"."calendar_date" AS "perd_d",
    "a"."efft_d" AS "efft_d",
    "a"."expy_d" AS "expy_d",
    "a"."vald_from_d" AS "vald_from_d",
    "a"."vald_to_d" AS "vald_to_d",
    "a"."rel_c" AS "rel_c",
    "a"."srce_syst_c" AS "srce_syst_c",
    "a"."row_secu_accs_c" AS "row_secu_accs_c",
    "a"."pros_key_i" AS "pros_key_i"
  FROM "%%vtech%%"."derv_prtf_paty_psst" AS "a"
  JOIN "%%vtech%%"."derv_prtf_paty_psst" AS "b"
    ON (
      "a"."int_grup_i" <> "b"."int_grup_i"
      OR "a"."join_from_d" <> "b"."join_from_d"
      OR "a"."join_to_d" <> "b"."join_to_d"
    )
    AND "a"."paty_i" = "b"."paty_i"
    AND "a"."rel_c" = "b"."rel_c"
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
      PARTITION BY "a"."paty_i", "a"."rel_c", "c"."calendar_date"
      ORDER BY "a"."efft_d" DESC NULLS LAST, "a"."int_grup_i" DESC NULLS LAST
    ) = 1
)
SELECT
  "dt2"."int_grup_i" AS "int_grup_i",
  "dt2"."paty_i" AS "paty_i",
  "dt2"."efft_d" AS "efft_d",
  "dt2"."expy_d" AS "expy_d",
  "dt2"."vald_from_d" AS "vald_from_d",
  "dt2"."vald_to_d" AS "vald_to_d",
  "dt2"."perd_d" AS "perd_d",
  "dt2"."rel_c" AS "rel_c",
  "dt2"."srce_syst_c" AS "srce_syst_c",
  ROW_NUMBER() OVER (PARTITION BY "dt2"."paty_i", "dt2"."rel_c" ORDER BY "dt2"."perd_d" NULLS FIRST) AS "grup_n",
  "dt2"."row_secu_accs_c" AS "row_secu_accs_c",
  "dt2"."pros_key_i" AS "pros_key_i"
FROM "dt2" AS "dt2"
```

#### 📊 SQL Metadata:
- **Tables**: CALENDAR, DERV_PRTF_PATY_INT_GRUP_PSST, DERV_PRTF_PATY_PSST
- **Columns**: CALENDAR_DATE, EFFT_D, EXPY_D, INT_GRUP_I, JOIN_FROM_D, JOIN_TO_D, PATY_I, PERD_D, PROS_KEY_I, REL_C, ROW_SECU_ACCS_C, SRCE_SYST_C, VALD_FROM_D, VALD_TO_D
- **Functions**: (CURRENT_DATE - EXTRACT(DAY FROM CURRENT_DATE) + 1), A.JOIN_FROM_D <> B.JOIN_FROM_D, A.JOIN_FROM_D <> B.JOIN_FROM_D OR A.JOIN_TO_D <> B.JOIN_TO_D, A.PATY_I = B.PATY_I, A.PATY_I = B.PATY_I AND A.REL_C = B.REL_C, A.PATY_I = B.PATY_I AND A.REL_C = B.REL_C AND (A.JOIN_FROM_D, A.JOIN_TO_D) OVERLAPS (B.JOIN_FROM_D, B.JOIN_TO_D), C.CALENDAR_DATE BETWEEN DT1.JOIN_FROM_D AND DT1.JOIN_TO_D, CURRENT_DATE, DAY, None
- **Window_Functions**: None

### SQL Block 3 (Lines 108-109)
- **Complexity Score**: 6
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
Delete from %%STARDATADB%%.DERV_PRTF_PATY_HIST_PSST All
;
```

#### ❄️ Converted Snowflake SQL:
```sql
DELETE FROM %%STARDATADB%%.DERV_PRTF_PATY_HIST_PSST AS All
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
DELETE FROM "%%stardatadb%%"."derv_prtf_paty_hist_psst" AS "all"
```

#### 📊 SQL Metadata:
- **Tables**: DERV_PRTF_PATY_HIST_PSST

### SQL Block 4 (Lines 113-169)
- **Complexity Score**: 243
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
Insert into %%STARDATADB%%.DERV_PRTF_PATY_HIST_PSST 
Select Distinct
   DT2.INT_GRUP_I
  ,DT2.PATY_I
  ,DT2.REL_C
  ,MIN(DT2.PERD_D) Over ( Partition By DT2.PATY_I, DT2.INT_GRUP_I, DT2.GRUP_N ) as JOIN_FROM_D
  ,MAX(DT2.PERD_D) Over ( Partition By DT2.PATY_I, DT2.INT_GRUP_I, DT2.GRUP_N ) as JOIN_TO_D
  ,DT2.VALD_FROM_D
  ,DT2.VALD_TO_D
  ,DT2.EFFT_D
  ,DT2.EXPY_D
  ,DT2.SRCE_SYST_C
  ,DT2.ROW_SECU_ACCS_C
  ,DT2.PROS_KEY_I
From
  (
  Select
     C.INT_GRUP_I
    ,C.PATY_I
    ,C.REL_C
    ,C.VALD_FROM_D
    ,C.VALD_TO_D
    ,C.EFFT_D
    ,C.EXPY_D
    ,C.SRCE_SYST_C
    ,C.ROW_SECU_ACCS_C
    ,C.PERD_D
    ,MAX(Coalesce( DT1.ROW_N, 0 )) Over (Partition By C.INT_GRUP_I, C.PATY_I, C.REL_C, C.PERD_D) as GRUP_N 	
    ,C.PROS_KEY_I
  From
    %%VTECH%%.DERV_PRTF_PATY_INT_GRUP_PSST C
    Left Join (
      -- Detect the change in non-key values between rows
      Select
         A.INT_GRUP_I
        ,A.PATY_I
        ,A.REL_C
        ,A.ROW_N
        ,A.PERD_D
        ,A.EFFT_D
        ,A.EXPY_D
      From
        %%VTECH%%.DERV_PRTF_PATY_INT_GRUP_PSST A
        Inner Join %%VTECH%%.DERV_PRTF_PATY_INT_GRUP_PSST B
        On A.PATY_I = B.PATY_I
        And A.REL_C = B.REL_C
        And A.ROW_N = B.ROW_N + 1
        And A.EFFT_D <> B.EFFT_D    
     
    ) DT1
    On C.INT_GRUP_I = DT1.INT_GRUP_I
    And C.PATY_I = DT1.PATY_I
    And C.REL_C = DT1.REL_C
    And C.ROW_N >= DT1.ROW_N
    And DT1.PERD_D <= C.PERD_D 
  ) DT2
;
```

#### ❄️ Converted Snowflake SQL:
```sql
INSERT INTO %%STARDATADB%%.DERV_PRTF_PATY_HIST_PSST SELECT DISTINCT DT2.INT_GRUP_I, DT2.PATY_I, DT2.REL_C, MIN(DT2.PERD_D) OVER (PARTITION BY DT2.PATY_I, DT2.INT_GRUP_I, DT2.GRUP_N) AS JOIN_FROM_D, MAX(DT2.PERD_D) OVER (PARTITION BY DT2.PATY_I, DT2.INT_GRUP_I, DT2.GRUP_N) AS JOIN_TO_D, DT2.VALD_FROM_D, DT2.VALD_TO_D, DT2.EFFT_D, DT2.EXPY_D, DT2.SRCE_SYST_C, DT2.ROW_SECU_ACCS_C, DT2.PROS_KEY_I FROM (SELECT C.INT_GRUP_I, C.PATY_I, C.REL_C, C.VALD_FROM_D, C.VALD_TO_D, C.EFFT_D, C.EXPY_D, C.SRCE_SYST_C, C.ROW_SECU_ACCS_C, C.PERD_D, MAX(COALESCE(DT1.ROW_N, 0)) OVER (PARTITION BY C.INT_GRUP_I, C.PATY_I, C.REL_C, C.PERD_D) AS GRUP_N, C.PROS_KEY_I FROM %%VTECH%%.DERV_PRTF_PATY_INT_GRUP_PSST AS C LEFT JOIN (SELECT A.INT_GRUP_I, A.PATY_I, A.REL_C, A.ROW_N, A.PERD_D, A.EFFT_D, A.EXPY_D FROM %%VTECH%%.DERV_PRTF_PATY_INT_GRUP_PSST AS A INNER JOIN %%VTECH%%.DERV_PRTF_PATY_INT_GRUP_PSST AS B ON A.PATY_I = B.PATY_I AND A.REL_C = B.REL_C AND A.ROW_N = B.ROW_N + 1 AND A.EFFT_D <> B.EFFT_D) AS DT1 ON C.INT_GRUP_I = DT1.INT_GRUP_I AND C.PATY_I = DT1.PATY_I AND C.REL_C = DT1.REL_C AND C.ROW_N >= DT1.ROW_N AND DT1.PERD_D <= C.PERD_D) AS DT2
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
INSERT INTO "%%stardatadb%%"."derv_prtf_paty_hist_psst"
WITH "dt2" AS (
  SELECT
    "c"."int_grup_i" AS "int_grup_i",
    "c"."paty_i" AS "paty_i",
    "c"."rel_c" AS "rel_c",
    "c"."vald_from_d" AS "vald_from_d",
    "c"."vald_to_d" AS "vald_to_d",
    "c"."efft_d" AS "efft_d",
    "c"."expy_d" AS "expy_d",
    "c"."srce_syst_c" AS "srce_syst_c",
    "c"."row_secu_accs_c" AS "row_secu_accs_c",
    "c"."perd_d" AS "perd_d",
    MAX(COALESCE("a"."row_n", 0)) OVER (PARTITION BY "c"."int_grup_i", "c"."paty_i", "c"."rel_c", "c"."perd_d") AS "grup_n",
    "c"."pros_key_i" AS "pros_key_i"
  FROM "%%vtech%%"."derv_prtf_paty_int_grup_psst" AS "c"
  LEFT JOIN "%%vtech%%"."derv_prtf_paty_int_grup_psst" AS "a"
    ON "a"."int_grup_i" = "c"."int_grup_i"
    AND "a"."paty_i" = "c"."paty_i"
    AND "a"."perd_d" <= "c"."perd_d"
    AND "a"."rel_c" = "c"."rel_c"
    AND "a"."row_n" <= "c"."row_n"
  JOIN "%%vtech%%"."derv_prtf_paty_int_grup_psst" AS "b"
    ON "a"."efft_d" <> "b"."efft_d"
    AND "a"."paty_i" = "b"."paty_i"
    AND "a"."rel_c" = "b"."rel_c"
    AND "a"."row_n" = "b"."row_n" + 1
)
SELECT DISTINCT
  "dt2"."int_grup_i" AS "int_grup_i",
  "dt2"."paty_i" AS "paty_i",
  "dt2"."rel_c" AS "rel_c",
  MIN("dt2"."perd_d") OVER (PARTITION BY "dt2"."paty_i", "dt2"."int_grup_i", "dt2"."grup_n") AS "join_from_d",
  MAX("dt2"."perd_d") OVER (PARTITION BY "dt2"."paty_i", "dt2"."int_grup_i", "dt2"."grup_n") AS "join_to_d",
  "dt2"."vald_from_d" AS "vald_from_d",
  "dt2"."vald_to_d" AS "vald_to_d",
  "dt2"."efft_d" AS "efft_d",
  "dt2"."expy_d" AS "expy_d",
  "dt2"."srce_syst_c" AS "srce_syst_c",
  "dt2"."row_secu_accs_c" AS "row_secu_accs_c",
  "dt2"."pros_key_i" AS "pros_key_i"
FROM "dt2" AS "dt2"
```

#### 📊 SQL Metadata:
- **Tables**: DERV_PRTF_PATY_HIST_PSST, DERV_PRTF_PATY_INT_GRUP_PSST
- **Columns**: EFFT_D, EXPY_D, GRUP_N, INT_GRUP_I, PATY_I, PERD_D, PROS_KEY_I, REL_C, ROW_N, ROW_SECU_ACCS_C, SRCE_SYST_C, VALD_FROM_D, VALD_TO_D
- **Functions**: A.PATY_I = B.PATY_I, A.PATY_I = B.PATY_I AND A.REL_C = B.REL_C, A.PATY_I = B.PATY_I AND A.REL_C = B.REL_C AND A.ROW_N = B.ROW_N + 1, C.INT_GRUP_I = DT1.INT_GRUP_I, C.INT_GRUP_I = DT1.INT_GRUP_I AND C.PATY_I = DT1.PATY_I, C.INT_GRUP_I = DT1.INT_GRUP_I AND C.PATY_I = DT1.PATY_I AND C.REL_C = DT1.REL_C, C.INT_GRUP_I = DT1.INT_GRUP_I AND C.PATY_I = DT1.PATY_I AND C.REL_C = DT1.REL_C AND C.ROW_N >= DT1.ROW_N, COALESCE(DT1.ROW_N, 0), DT1.ROW_N, DT2.PERD_D
- **Window_Functions**: COALESCE(DT1.ROW_N, 0), DT2.PERD_D

### SQL Block 5 (Lines 177-186)
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
	 %%STARDATADB%%.DERV_PRTF_PATY_PSST A
	,%%VTECH%%.DERV_PRTF_PATY_HIST_PSST B
Where  
  A.PATY_I = B.PATY_I
  And A.REL_C = B.REL_C
  And (A.JOIN_FROM_D,A.JOIN_TO_D) overlaps (B.JOIN_FROM_D,B.JOIN_TO_D)   
;
```

#### ❄️ Converted Snowflake SQL:
```sql
DELETE A FROM %%STARDATADB%%.DERV_PRTF_PATY_PSST AS A, %%VTECH%%.DERV_PRTF_PATY_HIST_PSST AS B WHERE A.PATY_I = B.PATY_I AND A.REL_C = B.REL_C AND (A.JOIN_FROM_D, A.JOIN_TO_D) OVERLAPS (B.JOIN_FROM_D, B.JOIN_TO_D)
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
DELETE   "a" FROM "%%stardatadb%%"."derv_prtf_paty_psst" AS "a"
  CROSS JOIN "%%vtech%%"."derv_prtf_paty_hist_psst" AS "b"
WHERE
  "a"."paty_i" = "b"."paty_i"
  AND "a"."rel_c" = "b"."rel_c"
  AND ("a"."join_from_d", "a"."join_to_d") OVERLAPS ("b"."join_from_d", "b"."join_to_d")
```

#### 📊 SQL Metadata:
- **Tables**: A, DERV_PRTF_PATY_HIST_PSST, DERV_PRTF_PATY_PSST
- **Columns**: JOIN_FROM_D, JOIN_TO_D, PATY_I, REL_C
- **Functions**: A.PATY_I = B.PATY_I, A.PATY_I = B.PATY_I AND A.REL_C = B.REL_C

### SQL Block 6 (Lines 194-214)
- **Complexity Score**: 0
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ❌ Invalid
- **Teradata Features**: 2

#### 📝 Original Teradata SQL:
```sql
Insert into %%STARDATADB%%.DERV_PRTF_PATY_PSST
Select
   INT_GRUP_I                    
  ,PATY_I                        
  ,JOIN_FROM_D                   
  ,(Case
      WHEN JOIN_TO_D = ADD_MONTHS(CURRENT_DATE, 1) Then ('9999-12-31'(Date,format'yyyy-mm-dd'))             
      ELSE JOIN_TO_D
    End
    ) as JOIN_TO_D                       
  ,EFFT_D                        
  ,EXPY_D                        
  ,VALD_FROM_D                   
  ,VALD_TO_D                     
  ,REL_C                         
  ,SRCE_SYST_C                   
  ,ROW_SECU_ACCS_C               
  ,PROS_KEY_I                                                 
From 
  %%VTECH%%.DERV_PRTF_PATY_HIST_PSST
;
```

#### ❄️ Converted Snowflake SQL:
```sql
Insert into %%STARDATADB%%.DERV_PRTF_PATY_PSST
Select
   INT_GRUP_I                    
  ,PATY_I                        
  ,JOIN_FROM_D                   
  ,(Case
      WHEN JOIN_TO_D = ADD_MONTHS(CURRENT_DATE, 1) Then ('9999-12-31'(Date,format'yyyy-mm-dd'))             
      ELSE JOIN_TO_D
    End
    ) as JOIN_TO_D                       
  ,EFFT_D                        
  ,EXPY_D                        
  ,VALD_FROM_D                   
  ,VALD_TO_D                     
  ,REL_C                         
  ,SRCE_SYST_C                   
  ,ROW_SECU_ACCS_C               
  ,PROS_KEY_I                                                 
From 
  %%VTECH%%.DERV_PRTF_PATY_HIST_PSST
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
