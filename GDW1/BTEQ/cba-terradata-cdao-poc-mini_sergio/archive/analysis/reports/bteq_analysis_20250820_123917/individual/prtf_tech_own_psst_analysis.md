# prtf_tech_own_psst.sql - BTEQ Analysis

## File Overview
- **File Name**: prtf_tech_own_psst.sql
- **Analysis Status**: ✅ Success
- **Control Statements**: 21
- **SQL Blocks**: 12

## Control Flow Analysis

| Line | Type | Statement |
|------|------|-----------|
| 1 | RUN | `.RUN FILE=%%BTEQ_LOGON_SCRIPT%%` |
| 2 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 29 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 84 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 87 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 94 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 163 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 166 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 184 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 208 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 215 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 270 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 273 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 283 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 354 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 357 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 374 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 398 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 401 | LOGOFF | `.LOGOFF` |
| 403 | LABEL | `.LABEL EXITERR` |
| 405 | LOGOFF | `.LOGOFF` |

## SQL Blocks Analysis

### SQL Block 1 (Lines 27-28)
- **Complexity Score**: 6
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
Delete from %%STARDATADB%%.DERV_PRTF_INT_GRUP_OWN_PSST All
;
```

#### ❄️ Converted Snowflake SQL:
```sql
DELETE FROM %%STARDATADB%%.DERV_PRTF_INT_GRUP_OWN_PSST AS All
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
DELETE FROM "%%stardatadb%%"."derv_prtf_int_grup_own_psst" AS "all"
```

#### 📊 SQL Metadata:
- **Tables**: DERV_PRTF_INT_GRUP_OWN_PSST

### SQL Block 2 (Lines 31-83)
- **Complexity Score**: 273
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 5

#### 📝 Original Teradata SQL:
```sql
Insert into %%STARDATADB%%.DERV_PRTF_INT_GRUP_OWN_PSST
Select
   DT2.INT_GRUP_I           as INT_GRUP_I
  ,DT2.ROLE_PLAY_I          as ROLE_PLAY_I
  ,DT2.EFFT_D               as EFFT_D
  ,DT2.EXPY_D               as EXPY_D
  ,DT2.VALD_FROM_D          as VALD_FROM_D
  ,DT2.VALD_TO_D            as VALD_TO_D
  ,DT2.PERD_D			          as PERD_D  
  ,DT2.ROLE_PLAY_TYPE_X     as ROLE_PLAY_TYPE_X
  ,DT2.DERV_PRTF_ROLE_C     as DERV_PRTF_ROLE_C
  ,DT2.SRCE_SYST_C          as SRCE_SYST_C
	,ROW_NUMBER() OVER (Partition By DT2.INT_GRUP_I Order by DT2.PERD_D, DT2.DERV_PRTF_ROLE_C )
  ,DT2.ROW_SECU_ACCS_C      as ROW_SECU_ACCS_C
  ,0                        as PROS_KEY_I
From
  (
     -- KEY =  (INT_GRUP_I, DERV_PRTF_ROLE_C)
    Select
       A.INT_GRUP_I           as INT_GRUP_I
      ,A.ROLE_PLAY_I               as ROLE_PLAY_I
      ,C.CALENDAR_DATE as PERD_D
      ,A.EFFT_D               as EFFT_D
      ,A.EXPY_D               as EXPY_D
      ,A.ROLE_PLAY_TYPE_X     as ROLE_PLAY_TYPE_X
      ,A.DERV_PRTF_ROLE_C     as DERV_PRTF_ROLE_C
      ,A.VALD_FROM_D
      ,A.VALD_TO_D
      ,A.SRCE_SYST_C          as SRCE_SYST_C
      ,A.ROW_SECU_ACCS_C      as ROW_SECU_ACCS_C
    From
      %%VTECH%%.DERV_PRTF_OWN_PSST A
      Inner Join %%VTECH%%.DERV_PRTF_OWN_PSST B    
      On A.INT_GRUP_I = B.INT_GRUP_I
      And A.DERV_PRTF_ROLE_C = B.DERV_PRTF_ROLE_C
      --And A.ROLE_PLAY_I <> B.ROLE_PLAY_I -- Same ROLE_PLAY_I already in step 1-4.
      And A.DERV_PRTF_ROLE_C = 'OWNR'
      And A.ROLE_PLAY_TYPE_X = B.ROLE_PLAY_TYPE_X -- Compare Empl to Empl and Dept to Dept only. What about ensuring that rule 5, use EMPL first!!
      -- Rule 5 seperate step, as the INT_GRUP_I values will be different
      And (A.JOIN_FROM_D,A.JOIN_TO_D) overlaps (B.JOIN_FROM_D,B.JOIN_TO_D)  
      And (
      	A.JOIN_FROM_D <> B.JOIN_FROM_D
      	Or A.JOIN_TO_D <> B.JOIN_TO_D
        Or A.ROLE_PLAY_I <> B.ROLE_PLAY_I
      )
 
			Inner Join %%VTECH%%.CALENDAR C
			On C.CALENDAR_DATE between A.JOIN_FROM_D and A.JOIN_TO_D
			And C.CALENDAR_DATE between ADD_MONTHS((Current_Date - EXTRACT(DAY from CURRENT_DATE) +1 ),-39) and ADD_MONTHS(Current_Date, 1)

		Qualify Row_Number() Over( Partition By A.INT_GRUP_I, A.ROLE_PLAY_TYPE_X, C.CALENDAR_DATE Order BY A.EFFT_D Desc, A.ROLE_PLAY_I Desc ) = 1
	) DT2
;
```

#### ❄️ Converted Snowflake SQL:
```sql
INSERT INTO %%STARDATADB%%.DERV_PRTF_INT_GRUP_OWN_PSST SELECT DT2.INT_GRUP_I AS INT_GRUP_I, DT2.ROLE_PLAY_I AS ROLE_PLAY_I, DT2.EFFT_D AS EFFT_D, DT2.EXPY_D AS EXPY_D, DT2.VALD_FROM_D AS VALD_FROM_D, DT2.VALD_TO_D AS VALD_TO_D, DT2.PERD_D AS PERD_D, DT2.ROLE_PLAY_TYPE_X AS ROLE_PLAY_TYPE_X, DT2.DERV_PRTF_ROLE_C AS DERV_PRTF_ROLE_C, DT2.SRCE_SYST_C AS SRCE_SYST_C, ROW_NUMBER() OVER (PARTITION BY DT2.INT_GRUP_I ORDER BY DT2.PERD_D NULLS FIRST, DT2.DERV_PRTF_ROLE_C NULLS FIRST), DT2.ROW_SECU_ACCS_C AS ROW_SECU_ACCS_C, 0 AS PROS_KEY_I FROM (SELECT A.INT_GRUP_I AS INT_GRUP_I, A.ROLE_PLAY_I AS ROLE_PLAY_I, C.CALENDAR_DATE AS PERD_D, A.EFFT_D AS EFFT_D, A.EXPY_D AS EXPY_D, A.ROLE_PLAY_TYPE_X AS ROLE_PLAY_TYPE_X, A.DERV_PRTF_ROLE_C AS DERV_PRTF_ROLE_C, A.VALD_FROM_D, A.VALD_TO_D, A.SRCE_SYST_C AS SRCE_SYST_C, A.ROW_SECU_ACCS_C AS ROW_SECU_ACCS_C FROM %%VTECH%%.DERV_PRTF_OWN_PSST AS A INNER JOIN %%VTECH%%.DERV_PRTF_OWN_PSST AS B ON A.INT_GRUP_I = B.INT_GRUP_I AND A.DERV_PRTF_ROLE_C = B.DERV_PRTF_ROLE_C AND A.DERV_PRTF_ROLE_C = 'OWNR' AND A.ROLE_PLAY_TYPE_X = B.ROLE_PLAY_TYPE_X AND (A.JOIN_FROM_D, A.JOIN_TO_D) OVERLAPS (B.JOIN_FROM_D, B.JOIN_TO_D) AND (A.JOIN_FROM_D <> B.JOIN_FROM_D OR A.JOIN_TO_D <> B.JOIN_TO_D OR A.ROLE_PLAY_I <> B.ROLE_PLAY_I) INNER JOIN %%VTECH%%.CALENDAR AS C ON C.CALENDAR_DATE BETWEEN A.JOIN_FROM_D AND A.JOIN_TO_D AND C.CALENDAR_DATE BETWEEN ADD_MONTHS((CURRENT_DATE - DATE_PART(DAY, CURRENT_DATE) + 1), -39) AND ADD_MONTHS(CURRENT_DATE, 1) QUALIFY ROW_NUMBER() OVER (PARTITION BY A.INT_GRUP_I, A.ROLE_PLAY_TYPE_X, C.CALENDAR_DATE ORDER BY A.EFFT_D DESC NULLS LAST, A.ROLE_PLAY_I DESC NULLS LAST) = 1) AS DT2
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
INSERT INTO "%%stardatadb%%"."derv_prtf_int_grup_own_psst"
WITH "dt2" AS (
  SELECT
    "a"."int_grup_i" AS "int_grup_i",
    "a"."role_play_i" AS "role_play_i",
    "c"."calendar_date" AS "perd_d",
    "a"."efft_d" AS "efft_d",
    "a"."expy_d" AS "expy_d",
    "a"."role_play_type_x" AS "role_play_type_x",
    "a"."derv_prtf_role_c" AS "derv_prtf_role_c",
    "a"."vald_from_d" AS "vald_from_d",
    "a"."vald_to_d" AS "vald_to_d",
    "a"."srce_syst_c" AS "srce_syst_c",
    "a"."row_secu_accs_c" AS "row_secu_accs_c"
  FROM "%%vtech%%"."derv_prtf_own_psst" AS "a"
  JOIN "%%vtech%%"."derv_prtf_own_psst" AS "b"
    ON "a"."derv_prtf_role_c" = "b"."derv_prtf_role_c"
    AND "a"."derv_prtf_role_c" = 'OWNR'
    AND "a"."int_grup_i" = "b"."int_grup_i"
    AND (
      "a"."join_from_d" <> "b"."join_from_d"
      OR "a"."join_to_d" <> "b"."join_to_d"
      OR "a"."role_play_i" <> "b"."role_play_i"
    )
    AND "a"."role_play_type_x" = "b"."role_play_type_x"
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
      PARTITION BY "a"."int_grup_i", "a"."role_play_type_x", "c"."calendar_date"
      ORDER BY "a"."efft_d" DESC NULLS LAST, "a"."role_play_i" DESC NULLS LAST
    ) = 1
)
SELECT
  "dt2"."int_grup_i" AS "int_grup_i",
  "dt2"."role_play_i" AS "role_play_i",
  "dt2"."efft_d" AS "efft_d",
  "dt2"."expy_d" AS "expy_d",
  "dt2"."vald_from_d" AS "vald_from_d",
  "dt2"."vald_to_d" AS "vald_to_d",
  "dt2"."perd_d" AS "perd_d",
  "dt2"."role_play_type_x" AS "role_play_type_x",
  "dt2"."derv_prtf_role_c" AS "derv_prtf_role_c",
  "dt2"."srce_syst_c" AS "srce_syst_c",
  ROW_NUMBER() OVER (
    PARTITION BY "dt2"."int_grup_i"
    ORDER BY "dt2"."perd_d" NULLS FIRST, "dt2"."derv_prtf_role_c" NULLS FIRST
  ) AS "_col_10",
  "dt2"."row_secu_accs_c" AS "row_secu_accs_c",
  0 AS "pros_key_i"
FROM "dt2" AS "dt2"
```

#### 📊 SQL Metadata:
- **Tables**: CALENDAR, DERV_PRTF_INT_GRUP_OWN_PSST, DERV_PRTF_OWN_PSST
- **Columns**: CALENDAR_DATE, DERV_PRTF_ROLE_C, EFFT_D, EXPY_D, INT_GRUP_I, JOIN_FROM_D, JOIN_TO_D, PERD_D, ROLE_PLAY_I, ROLE_PLAY_TYPE_X, ROW_SECU_ACCS_C, SRCE_SYST_C, VALD_FROM_D, VALD_TO_D
- **Functions**: (CURRENT_DATE - EXTRACT(DAY FROM CURRENT_DATE) + 1), A.INT_GRUP_I = B.INT_GRUP_I, A.INT_GRUP_I = B.INT_GRUP_I AND A.DERV_PRTF_ROLE_C = B.DERV_PRTF_ROLE_C, A.INT_GRUP_I = B.INT_GRUP_I AND A.DERV_PRTF_ROLE_C = B.DERV_PRTF_ROLE_C AND A.DERV_PRTF_ROLE_C = 'OWNR', A.INT_GRUP_I = B.INT_GRUP_I AND A.DERV_PRTF_ROLE_C = B.DERV_PRTF_ROLE_C AND A.DERV_PRTF_ROLE_C = 'OWNR' AND A.ROLE_PLAY_TYPE_X = B.ROLE_PLAY_TYPE_X, A.INT_GRUP_I = B.INT_GRUP_I AND A.DERV_PRTF_ROLE_C = B.DERV_PRTF_ROLE_C AND A.DERV_PRTF_ROLE_C = 'OWNR' AND A.ROLE_PLAY_TYPE_X = B.ROLE_PLAY_TYPE_X AND (A.JOIN_FROM_D, A.JOIN_TO_D) OVERLAPS (B.JOIN_FROM_D, B.JOIN_TO_D), A.JOIN_FROM_D <> B.JOIN_FROM_D, A.JOIN_FROM_D <> B.JOIN_FROM_D OR A.JOIN_TO_D <> B.JOIN_TO_D, C.CALENDAR_DATE BETWEEN A.JOIN_FROM_D AND A.JOIN_TO_D, CURRENT_DATE, DAY, None
- **Window_Functions**: None

### SQL Block 3 (Lines 92-93)
- **Complexity Score**: 4
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
Delete from %%STARDATADB%%.DERV_PRTF_OWN_HIST_PSST 
;
```

#### ❄️ Converted Snowflake SQL:
```sql
DELETE FROM %%STARDATADB%%.DERV_PRTF_OWN_HIST_PSST
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
DELETE FROM "%%stardatadb%%"."derv_prtf_own_hist_psst"
```

#### 📊 SQL Metadata:
- **Tables**: DERV_PRTF_OWN_HIST_PSST

### SQL Block 4 (Lines 96-162)
- **Complexity Score**: 273
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
Insert into %%STARDATADB%%.DERV_PRTF_OWN_HIST_PSST 
Select Distinct
	 DT2.INT_GRUP_I                    
	,DT2.ROLE_PLAY_I 
  ,MIN(DT2.PERD_D) Over ( Partition By DT2.INT_GRUP_I, DT2.ROLE_PLAY_I, DT2.GRUP_N ) as JOIN_FROM_D
  ,MAX(DT2.PERD_D) Over ( Partition By DT2.INT_GRUP_I, DT2.ROLE_PLAY_I, DT2.GRUP_N ) as JOIN_TO_D	
	,DT2.EFFT_D                        
	,DT2.EXPY_D   
	,DT2.VALD_FROM_D
	,DT2.VALD_TO_D  
  ,DT2.ROLE_PLAY_TYPE_X              
	,DT2.DERV_PRTF_ROLE_C              
	,DT2.SRCE_SYST_C    
	,DT2.ROW_SECU_ACCS_C               
	,DT2.PROS_KEY_I                    
From
  (
		Select Distinct
			 C.INT_GRUP_I                    
			,C.ROLE_PLAY_I                   
			,C.EFFT_D                        
			,C.EXPY_D   
			,C.VALD_FROM_D
			,C.VALD_TO_D  
			,C.PERD_D                        
			,C.ROLE_PLAY_TYPE_X              
			,C.DERV_PRTF_ROLE_C              
			,C.SRCE_SYST_C     
			,C.ROW_N
			,C.ROW_SECU_ACCS_C               
			,C.PROS_KEY_I                    
      ,MAX(Coalesce( DT1.ROW_N, 0 )) Over (Partition By C.INT_GRUP_I, C.ROLE_PLAY_I, C.PERD_D) as GRUP_N 
		From
			%%VTECH%%.DERV_PRTF_INT_GRUP_OWN_PSST C
			Left Join (
				-- Detect the change in non-key values between rows
				Select
           A.INT_GRUP_I
          ,A.ROLE_PLAY_I
          ,A.DERV_PRTF_ROLE_C
          ,A.ROLE_PLAY_TYPE_X
          ,A.ROW_N
          ,A.PERD_D
          ,A.EFFT_D
				From
					%%VTECH%%.DERV_PRTF_INT_GRUP_OWN_PSST A
					Inner Join %%VTECH%%.DERV_PRTF_INT_GRUP_OWN_PSST B
					
					On A.INT_GRUP_I = B.INT_GRUP_I
					And A.ROW_N = B.ROW_N + 1					
					And A.DERV_PRTF_ROLE_C = B.DERV_PRTF_ROLE_C					
          And A.DERV_PRTF_ROLE_C = 'OWNR'
          And A.ROLE_PLAY_TYPE_X = B.ROLE_PLAY_TYPE_X
          And ( 
            A.ROLE_PLAY_I <> B.ROLE_PLAY_I
  					Or  A.EFFT_D <> B.EFFT_D
					)

			) DT1
			On C.INT_GRUP_I = DT1.INT_GRUP_I
			And C.ROLE_PLAY_TYPE_X = DT1.ROLE_PLAY_TYPE_X
			And C.EFFT_D = DT1.EFFT_D
      And C.ROW_N >= DT1.ROW_N
      And DT1.PERD_D <= C.PERD_D 

  ) DT2
;
```

#### ❄️ Converted Snowflake SQL:
```sql
INSERT INTO %%STARDATADB%%.DERV_PRTF_OWN_HIST_PSST SELECT DISTINCT DT2.INT_GRUP_I, DT2.ROLE_PLAY_I, MIN(DT2.PERD_D) OVER (PARTITION BY DT2.INT_GRUP_I, DT2.ROLE_PLAY_I, DT2.GRUP_N) AS JOIN_FROM_D, MAX(DT2.PERD_D) OVER (PARTITION BY DT2.INT_GRUP_I, DT2.ROLE_PLAY_I, DT2.GRUP_N) AS JOIN_TO_D, DT2.EFFT_D, DT2.EXPY_D, DT2.VALD_FROM_D, DT2.VALD_TO_D, DT2.ROLE_PLAY_TYPE_X, DT2.DERV_PRTF_ROLE_C, DT2.SRCE_SYST_C, DT2.ROW_SECU_ACCS_C, DT2.PROS_KEY_I FROM (SELECT DISTINCT C.INT_GRUP_I, C.ROLE_PLAY_I, C.EFFT_D, C.EXPY_D, C.VALD_FROM_D, C.VALD_TO_D, C.PERD_D, C.ROLE_PLAY_TYPE_X, C.DERV_PRTF_ROLE_C, C.SRCE_SYST_C, C.ROW_N, C.ROW_SECU_ACCS_C, C.PROS_KEY_I, MAX(COALESCE(DT1.ROW_N, 0)) OVER (PARTITION BY C.INT_GRUP_I, C.ROLE_PLAY_I, C.PERD_D) AS GRUP_N FROM %%VTECH%%.DERV_PRTF_INT_GRUP_OWN_PSST AS C LEFT JOIN (SELECT A.INT_GRUP_I, A.ROLE_PLAY_I, A.DERV_PRTF_ROLE_C, A.ROLE_PLAY_TYPE_X, A.ROW_N, A.PERD_D, A.EFFT_D FROM %%VTECH%%.DERV_PRTF_INT_GRUP_OWN_PSST AS A INNER JOIN %%VTECH%%.DERV_PRTF_INT_GRUP_OWN_PSST AS B ON A.INT_GRUP_I = B.INT_GRUP_I AND A.ROW_N = B.ROW_N + 1 AND A.DERV_PRTF_ROLE_C = B.DERV_PRTF_ROLE_C AND A.DERV_PRTF_ROLE_C = 'OWNR' AND A.ROLE_PLAY_TYPE_X = B.ROLE_PLAY_TYPE_X AND (A.ROLE_PLAY_I <> B.ROLE_PLAY_I OR A.EFFT_D <> B.EFFT_D)) AS DT1 ON C.INT_GRUP_I = DT1.INT_GRUP_I AND C.ROLE_PLAY_TYPE_X = DT1.ROLE_PLAY_TYPE_X AND C.EFFT_D = DT1.EFFT_D AND C.ROW_N >= DT1.ROW_N AND DT1.PERD_D <= C.PERD_D) AS DT2
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
INSERT INTO "%%stardatadb%%"."derv_prtf_own_hist_psst"
WITH "dt2" AS (
  SELECT DISTINCT
    "c"."int_grup_i" AS "int_grup_i",
    "c"."role_play_i" AS "role_play_i",
    "c"."efft_d" AS "efft_d",
    "c"."expy_d" AS "expy_d",
    "c"."vald_from_d" AS "vald_from_d",
    "c"."vald_to_d" AS "vald_to_d",
    "c"."perd_d" AS "perd_d",
    "c"."role_play_type_x" AS "role_play_type_x",
    "c"."derv_prtf_role_c" AS "derv_prtf_role_c",
    "c"."srce_syst_c" AS "srce_syst_c",
    "c"."row_n" AS "row_n",
    "c"."row_secu_accs_c" AS "row_secu_accs_c",
    "c"."pros_key_i" AS "pros_key_i",
    MAX(COALESCE("a"."row_n", 0)) OVER (PARTITION BY "c"."int_grup_i", "c"."role_play_i", "c"."perd_d") AS "grup_n"
  FROM "%%vtech%%"."derv_prtf_int_grup_own_psst" AS "c"
  LEFT JOIN "%%vtech%%"."derv_prtf_int_grup_own_psst" AS "a"
    ON "a"."efft_d" = "c"."efft_d"
    AND "a"."int_grup_i" = "c"."int_grup_i"
    AND "a"."perd_d" <= "c"."perd_d"
    AND "a"."role_play_type_x" = "c"."role_play_type_x"
    AND "a"."row_n" <= "c"."row_n"
  JOIN "%%vtech%%"."derv_prtf_int_grup_own_psst" AS "b"
    ON "a"."derv_prtf_role_c" = "b"."derv_prtf_role_c"
    AND "a"."derv_prtf_role_c" = 'OWNR'
    AND (
      "a"."efft_d" <> "b"."efft_d" OR "a"."role_play_i" <> "b"."role_play_i"
    )
    AND "a"."int_grup_i" = "b"."int_grup_i"
    AND "a"."role_play_type_x" = "b"."role_play_type_x"
    AND "a"."row_n" = "b"."row_n" + 1
)
SELECT DISTINCT
  "dt2"."int_grup_i" AS "int_grup_i",
  "dt2"."role_play_i" AS "role_play_i",
  MIN("dt2"."perd_d") OVER (PARTITION BY "dt2"."int_grup_i", "dt2"."role_play_i", "dt2"."grup_n") AS "join_from_d",
  MAX("dt2"."perd_d") OVER (PARTITION BY "dt2"."int_grup_i", "dt2"."role_play_i", "dt2"."grup_n") AS "join_to_d",
  "dt2"."efft_d" AS "efft_d",
  "dt2"."expy_d" AS "expy_d",
  "dt2"."vald_from_d" AS "vald_from_d",
  "dt2"."vald_to_d" AS "vald_to_d",
  "dt2"."role_play_type_x" AS "role_play_type_x",
  "dt2"."derv_prtf_role_c" AS "derv_prtf_role_c",
  "dt2"."srce_syst_c" AS "srce_syst_c",
  "dt2"."row_secu_accs_c" AS "row_secu_accs_c",
  "dt2"."pros_key_i" AS "pros_key_i"
FROM "dt2" AS "dt2"
```

#### 📊 SQL Metadata:
- **Tables**: DERV_PRTF_INT_GRUP_OWN_PSST, DERV_PRTF_OWN_HIST_PSST
- **Columns**: DERV_PRTF_ROLE_C, EFFT_D, EXPY_D, GRUP_N, INT_GRUP_I, PERD_D, PROS_KEY_I, ROLE_PLAY_I, ROLE_PLAY_TYPE_X, ROW_N, ROW_SECU_ACCS_C, SRCE_SYST_C, VALD_FROM_D, VALD_TO_D
- **Functions**: A.INT_GRUP_I = B.INT_GRUP_I, A.INT_GRUP_I = B.INT_GRUP_I AND A.ROW_N = B.ROW_N + 1, A.INT_GRUP_I = B.INT_GRUP_I AND A.ROW_N = B.ROW_N + 1 AND A.DERV_PRTF_ROLE_C = B.DERV_PRTF_ROLE_C, A.INT_GRUP_I = B.INT_GRUP_I AND A.ROW_N = B.ROW_N + 1 AND A.DERV_PRTF_ROLE_C = B.DERV_PRTF_ROLE_C AND A.DERV_PRTF_ROLE_C = 'OWNR', A.INT_GRUP_I = B.INT_GRUP_I AND A.ROW_N = B.ROW_N + 1 AND A.DERV_PRTF_ROLE_C = B.DERV_PRTF_ROLE_C AND A.DERV_PRTF_ROLE_C = 'OWNR' AND A.ROLE_PLAY_TYPE_X = B.ROLE_PLAY_TYPE_X, A.ROLE_PLAY_I <> B.ROLE_PLAY_I, C.INT_GRUP_I = DT1.INT_GRUP_I, C.INT_GRUP_I = DT1.INT_GRUP_I AND C.ROLE_PLAY_TYPE_X = DT1.ROLE_PLAY_TYPE_X, C.INT_GRUP_I = DT1.INT_GRUP_I AND C.ROLE_PLAY_TYPE_X = DT1.ROLE_PLAY_TYPE_X AND C.EFFT_D = DT1.EFFT_D, C.INT_GRUP_I = DT1.INT_GRUP_I AND C.ROLE_PLAY_TYPE_X = DT1.ROLE_PLAY_TYPE_X AND C.EFFT_D = DT1.EFFT_D AND C.ROW_N >= DT1.ROW_N, COALESCE(DT1.ROW_N, 0), DT1.ROW_N, DT2.PERD_D
- **Window_Functions**: COALESCE(DT1.ROW_N, 0), DT2.PERD_D

### SQL Block 5 (Lines 173-183)
- **Complexity Score**: 54
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
Delete
	A
From 
	 %%STARDATADB%%.DERV_PRTF_OWN_PSST A
	,%%VTECH%%.DERV_PRTF_OWN_HIST_PSST B
Where  
  A.INT_GRUP_I = B.INT_GRUP_I
  And A.ROLE_PLAY_TYPE_X = B.ROLE_PLAY_TYPE_X        
  And A.DERV_PRTF_ROLE_C = B.DERV_PRTF_ROLE_C        
  And (A.JOIN_FROM_D,A.JOIN_TO_D) overlaps (B.JOIN_FROM_D,B.JOIN_TO_D)     
;
```

#### ❄️ Converted Snowflake SQL:
```sql
DELETE A FROM %%STARDATADB%%.DERV_PRTF_OWN_PSST AS A, %%VTECH%%.DERV_PRTF_OWN_HIST_PSST AS B WHERE A.INT_GRUP_I = B.INT_GRUP_I AND A.ROLE_PLAY_TYPE_X = B.ROLE_PLAY_TYPE_X AND A.DERV_PRTF_ROLE_C = B.DERV_PRTF_ROLE_C AND (A.JOIN_FROM_D, A.JOIN_TO_D) OVERLAPS (B.JOIN_FROM_D, B.JOIN_TO_D)
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
DELETE   "a" FROM "%%stardatadb%%"."derv_prtf_own_psst" AS "a"
  CROSS JOIN "%%vtech%%"."derv_prtf_own_hist_psst" AS "b"
WHERE
  "a"."derv_prtf_role_c" = "b"."derv_prtf_role_c"
  AND "a"."int_grup_i" = "b"."int_grup_i"
  AND "a"."role_play_type_x" = "b"."role_play_type_x"
  AND ("a"."join_from_d", "a"."join_to_d") OVERLAPS ("b"."join_from_d", "b"."join_to_d")
```

#### 📊 SQL Metadata:
- **Tables**: A, DERV_PRTF_OWN_HIST_PSST, DERV_PRTF_OWN_PSST
- **Columns**: DERV_PRTF_ROLE_C, INT_GRUP_I, JOIN_FROM_D, JOIN_TO_D, ROLE_PLAY_TYPE_X
- **Functions**: A.INT_GRUP_I = B.INT_GRUP_I, A.INT_GRUP_I = B.INT_GRUP_I AND A.ROLE_PLAY_TYPE_X = B.ROLE_PLAY_TYPE_X, A.INT_GRUP_I = B.INT_GRUP_I AND A.ROLE_PLAY_TYPE_X = B.ROLE_PLAY_TYPE_X AND A.DERV_PRTF_ROLE_C = B.DERV_PRTF_ROLE_C

### SQL Block 6 (Lines 186-207)
- **Complexity Score**: 0
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ❌ Invalid
- **Teradata Features**: 2

#### 📝 Original Teradata SQL:
```sql
Insert into %%STARDATADB%%.DERV_PRTF_OWN_PSST
Select
   INT_GRUP_I                    
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
  ,DERV_PRTF_ROLE_C              
  ,ROLE_PLAY_TYPE_X              
  ,ROLE_PLAY_I                   
  ,SRCE_SYST_C                   
  ,ROW_SECU_ACCS_C               
  ,PROS_KEY_I                      
From 
  %%VTECH%%.DERV_PRTF_OWN_HIST_PSST
;
```

#### ❄️ Converted Snowflake SQL:
```sql
Insert into %%STARDATADB%%.DERV_PRTF_OWN_PSST
Select
   INT_GRUP_I                    
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
  ,DERV_PRTF_ROLE_C              
  ,ROLE_PLAY_TYPE_X              
  ,ROLE_PLAY_I                   
  ,SRCE_SYST_C                   
  ,ROW_SECU_ACCS_C               
  ,PROS_KEY_I                      
From 
  %%VTECH%%.DERV_PRTF_OWN_HIST_PSST
;
```

#### 🔍 Syntax Validation Details:
- **Valid**: ❌

#### 🎯 Teradata Features Detected:
- ADD_MONTHS function
- Variable substitution

### SQL Block 7 (Lines 213-214)
- **Complexity Score**: 6
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
Delete from %%STARDATADB%%.DERV_PRTF_INT_GRUP_OWN_PSST All
;
```

#### ❄️ Converted Snowflake SQL:
```sql
DELETE FROM %%STARDATADB%%.DERV_PRTF_INT_GRUP_OWN_PSST AS All
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
DELETE FROM "%%stardatadb%%"."derv_prtf_int_grup_own_psst" AS "all"
```

#### 📊 SQL Metadata:
- **Tables**: DERV_PRTF_INT_GRUP_OWN_PSST

### SQL Block 8 (Lines 217-269)
- **Complexity Score**: 261
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 5

#### 📝 Original Teradata SQL:
```sql
Insert into %%STARDATADB%%.DERV_PRTF_INT_GRUP_OWN_PSST
Select
   DT2.INT_GRUP_I           as INT_GRUP_I
  ,DT2.ROLE_PLAY_I          as ROLE_PLAY_I
  ,DT2.EFFT_D               as EFFT_D
  ,DT2.EXPY_D               as EXPY_D
  ,DT2.VALD_FROM_D          as VALD_FROM_D
  ,DT2.VALD_TO_D            as VALD_TO_D
  ,DT2.PERD_D				        as PERD_D  
  ,DT2.ROLE_PLAY_TYPE_X     as ROLE_PLAY_TYPE_X
  ,DT2.DERV_PRTF_ROLE_C     as DERV_PRTF_ROLE_C
  ,DT2.SRCE_SYST_C          as SRCE_SYST_C
	,ROW_NUMBER() OVER (Partition By DT2.INT_GRUP_I, DT2.DERV_PRTF_ROLE_C Order by  DT2.PERD_D, DT2.ROLE_PLAY_I Desc)
  ,DT2.ROW_SECU_ACCS_C      as ROW_SECU_ACCS_C
  ,0                        as PROS_KEY_I
From
	(
    Select 
       A.INT_GRUP_I           as INT_GRUP_I
      ,A.ROLE_PLAY_I
      ,A.DERV_PRTF_ROLE_C		
      ,C.CALENDAR_DATE as PERD_D			
      ,A.EFFT_D               as EFFT_D
      ,A.EXPY_D               as EXPY_D
      ,A.ROLE_PLAY_TYPE_X --'Employee'             
      ,A.VALD_FROM_D
      ,A.VALD_TO_D
      ,A.SRCE_SYST_C          as SRCE_SYST_C
      ,A.ROW_SECU_ACCS_C      as ROW_SECU_ACCS_C
    From
      /* KEY = (INT_GRUP_I, ROLE_PLAY_I) */
      %%VTECH%%.DERV_PRTF_OWN_PSST A
      Inner Join %%VTECH%%.DERV_PRTF_OWN_PSST B
    
      On A.INT_GRUP_I = B.INT_GRUP_I	       
      And A.DERV_PRTF_ROLE_C = B.DERV_PRTF_ROLE_C						
      And A.DERV_PRTF_ROLE_C = 'OWNR'
      And A.ROLE_PLAY_TYPE_X <> B.ROLE_PLAY_TYPE_X
      And (A.JOIN_FROM_D,A.JOIN_TO_D) overlaps (B.JOIN_FROM_D,B.JOIN_TO_D) 
      --And (
      --	A.JOIN_FROM_D <> B.JOIN_FROM_D
      --	Or A.JOIN_TO_D <> B.JOIN_TO_D
      --)   

      Inner Join %%VTECH%%.CALENDAR C
      --On C.CALENDAR_DATE between A.VALD_FROM_D and A.VALD_TO_D
      On C.CALENDAR_DATE between A.JOIN_FROM_D and A.JOIN_TO_D
      And C.CALENDAR_DATE between ADD_MONTHS((Current_Date - EXTRACT(DAY from CURRENT_DATE) +1 ),-39) and ADD_MONTHS(Current_Date, 1)

    Qualify Row_Number() Over( Partition By A.INT_GRUP_I, A.DERV_PRTF_ROLE_C, C.CALENDAR_DATE Order BY A.EFFT_D Desc, A.ROLE_PLAY_TYPE_X Desc, A.ROLE_PLAY_I Desc ) = 1    
    Group By 1,2,3,4,5,6,7,8,9,10,11				
	) DT2
;
```

#### ❄️ Converted Snowflake SQL:
```sql
INSERT INTO %%STARDATADB%%.DERV_PRTF_INT_GRUP_OWN_PSST SELECT DT2.INT_GRUP_I AS INT_GRUP_I, DT2.ROLE_PLAY_I AS ROLE_PLAY_I, DT2.EFFT_D AS EFFT_D, DT2.EXPY_D AS EXPY_D, DT2.VALD_FROM_D AS VALD_FROM_D, DT2.VALD_TO_D AS VALD_TO_D, DT2.PERD_D AS PERD_D, DT2.ROLE_PLAY_TYPE_X AS ROLE_PLAY_TYPE_X, DT2.DERV_PRTF_ROLE_C AS DERV_PRTF_ROLE_C, DT2.SRCE_SYST_C AS SRCE_SYST_C, ROW_NUMBER() OVER (PARTITION BY DT2.INT_GRUP_I, DT2.DERV_PRTF_ROLE_C ORDER BY DT2.PERD_D NULLS FIRST, DT2.ROLE_PLAY_I DESC NULLS LAST), DT2.ROW_SECU_ACCS_C AS ROW_SECU_ACCS_C, 0 AS PROS_KEY_I FROM (SELECT A.INT_GRUP_I AS INT_GRUP_I, A.ROLE_PLAY_I, A.DERV_PRTF_ROLE_C, C.CALENDAR_DATE AS PERD_D, A.EFFT_D AS EFFT_D, A.EXPY_D AS EXPY_D, A.ROLE_PLAY_TYPE_X, A.VALD_FROM_D, A.VALD_TO_D, A.SRCE_SYST_C AS SRCE_SYST_C, A.ROW_SECU_ACCS_C AS ROW_SECU_ACCS_C FROM %%VTECH%%.DERV_PRTF_OWN_PSST AS A INNER JOIN %%VTECH%%.DERV_PRTF_OWN_PSST AS B ON A.INT_GRUP_I = B.INT_GRUP_I AND A.DERV_PRTF_ROLE_C = B.DERV_PRTF_ROLE_C AND A.DERV_PRTF_ROLE_C = 'OWNR' AND A.ROLE_PLAY_TYPE_X <> B.ROLE_PLAY_TYPE_X AND (A.JOIN_FROM_D, A.JOIN_TO_D) OVERLAPS (B.JOIN_FROM_D, B.JOIN_TO_D) INNER JOIN %%VTECH%%.CALENDAR AS C ON C.CALENDAR_DATE BETWEEN A.JOIN_FROM_D AND A.JOIN_TO_D AND C.CALENDAR_DATE BETWEEN ADD_MONTHS((CURRENT_DATE - DATE_PART(DAY, CURRENT_DATE) + 1), -39) AND ADD_MONTHS(CURRENT_DATE, 1) GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 QUALIFY ROW_NUMBER() OVER (PARTITION BY A.INT_GRUP_I, A.DERV_PRTF_ROLE_C, C.CALENDAR_DATE ORDER BY A.EFFT_D DESC NULLS LAST, A.ROLE_PLAY_TYPE_X DESC NULLS LAST, A.ROLE_PLAY_I DESC NULLS LAST) = 1) AS DT2
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
INSERT INTO "%%stardatadb%%"."derv_prtf_int_grup_own_psst"
WITH "dt2" AS (
  SELECT
    "a"."int_grup_i" AS "int_grup_i",
    "a"."role_play_i" AS "role_play_i",
    "a"."derv_prtf_role_c" AS "derv_prtf_role_c",
    "c"."calendar_date" AS "perd_d",
    "a"."efft_d" AS "efft_d",
    "a"."expy_d" AS "expy_d",
    "a"."role_play_type_x" AS "role_play_type_x",
    "a"."vald_from_d" AS "vald_from_d",
    "a"."vald_to_d" AS "vald_to_d",
    "a"."srce_syst_c" AS "srce_syst_c",
    "a"."row_secu_accs_c" AS "row_secu_accs_c"
  FROM "%%vtech%%"."derv_prtf_own_psst" AS "a"
  JOIN "%%vtech%%"."derv_prtf_own_psst" AS "b"
    ON "a"."derv_prtf_role_c" = "b"."derv_prtf_role_c"
    AND "a"."derv_prtf_role_c" = 'OWNR'
    AND "a"."int_grup_i" = "b"."int_grup_i"
    AND "a"."role_play_type_x" <> "b"."role_play_type_x"
    AND ("a"."join_from_d", "a"."join_to_d") OVERLAPS ("b"."join_from_d", "b"."join_to_d")
  JOIN "%%vtech%%"."calendar" AS "c"
    ON "a"."join_from_d" <= "c"."calendar_date"
    AND "a"."join_to_d" >= "c"."calendar_date"
    AND "c"."calendar_date" <= ADD_MONTHS(CURRENT_DATE, 1)
    AND "c"."calendar_date" >= ADD_MONTHS((
      CURRENT_DATE - DATE_PART(DAY, CURRENT_DATE) + 1
    ), -39)
  GROUP BY
    "a"."int_grup_i",
    "a"."role_play_i",
    "a"."derv_prtf_role_c",
    "c"."calendar_date",
    "a"."efft_d",
    "a"."expy_d",
    "a"."role_play_type_x",
    "a"."vald_from_d",
    "a"."vald_to_d",
    "a"."srce_syst_c",
    "a"."row_secu_accs_c"
  QUALIFY
    ROW_NUMBER() OVER (
      PARTITION BY "a"."int_grup_i", "a"."derv_prtf_role_c", "c"."calendar_date"
      ORDER BY "a"."efft_d" DESC NULLS LAST, "a"."role_play_type_x" DESC NULLS LAST, "a"."role_play_i" DESC NULLS LAST
    ) = 1
)
SELECT
  "dt2"."int_grup_i" AS "int_grup_i",
  "dt2"."role_play_i" AS "role_play_i",
  "dt2"."efft_d" AS "efft_d",
  "dt2"."expy_d" AS "expy_d",
  "dt2"."vald_from_d" AS "vald_from_d",
  "dt2"."vald_to_d" AS "vald_to_d",
  "dt2"."perd_d" AS "perd_d",
  "dt2"."role_play_type_x" AS "role_play_type_x",
  "dt2"."derv_prtf_role_c" AS "derv_prtf_role_c",
  "dt2"."srce_syst_c" AS "srce_syst_c",
  ROW_NUMBER() OVER (
    PARTITION BY "dt2"."int_grup_i", "dt2"."derv_prtf_role_c"
    ORDER BY "dt2"."perd_d" NULLS FIRST, "dt2"."role_play_i" DESC NULLS LAST
  ) AS "_col_10",
  "dt2"."row_secu_accs_c" AS "row_secu_accs_c",
  0 AS "pros_key_i"
FROM "dt2" AS "dt2"
```

#### 📊 SQL Metadata:
- **Tables**: CALENDAR, DERV_PRTF_INT_GRUP_OWN_PSST, DERV_PRTF_OWN_PSST
- **Columns**: CALENDAR_DATE, DERV_PRTF_ROLE_C, EFFT_D, EXPY_D, INT_GRUP_I, JOIN_FROM_D, JOIN_TO_D, PERD_D, ROLE_PLAY_I, ROLE_PLAY_TYPE_X, ROW_SECU_ACCS_C, SRCE_SYST_C, VALD_FROM_D, VALD_TO_D
- **Functions**: (CURRENT_DATE - EXTRACT(DAY FROM CURRENT_DATE) + 1), A.INT_GRUP_I = B.INT_GRUP_I, A.INT_GRUP_I = B.INT_GRUP_I AND A.DERV_PRTF_ROLE_C = B.DERV_PRTF_ROLE_C, A.INT_GRUP_I = B.INT_GRUP_I AND A.DERV_PRTF_ROLE_C = B.DERV_PRTF_ROLE_C AND A.DERV_PRTF_ROLE_C = 'OWNR', A.INT_GRUP_I = B.INT_GRUP_I AND A.DERV_PRTF_ROLE_C = B.DERV_PRTF_ROLE_C AND A.DERV_PRTF_ROLE_C = 'OWNR' AND A.ROLE_PLAY_TYPE_X <> B.ROLE_PLAY_TYPE_X, C.CALENDAR_DATE BETWEEN A.JOIN_FROM_D AND A.JOIN_TO_D, CURRENT_DATE, DAY, None
- **Window_Functions**: None

### SQL Block 9 (Lines 281-282)
- **Complexity Score**: 4
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
Delete from %%STARDATADB%%.DERV_PRTF_OWN_HIST_PSST 
;
```

#### ❄️ Converted Snowflake SQL:
```sql
DELETE FROM %%STARDATADB%%.DERV_PRTF_OWN_HIST_PSST
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
DELETE FROM "%%stardatadb%%"."derv_prtf_own_hist_psst"
```

#### 📊 SQL Metadata:
- **Tables**: DERV_PRTF_OWN_HIST_PSST

### SQL Block 10 (Lines 285-353)
- **Complexity Score**: 275
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
Insert into %%STARDATADB%%.DERV_PRTF_OWN_HIST_PSST 
Select Distinct
	 DT2.INT_GRUP_I                    
	,DT2.ROLE_PLAY_I 
  ,MIN(DT2.PERD_D) Over ( Partition By DT2.INT_GRUP_I, DT2.ROLE_PLAY_I, DT2.GRUP_N ) as JOIN_FROM_D
  ,MAX(DT2.PERD_D) Over ( Partition By DT2.INT_GRUP_I, DT2.ROLE_PLAY_I, DT2.GRUP_N ) as JOIN_TO_D	
	,DT2.EFFT_D                        
	,DT2.EXPY_D   
	,DT2.VALD_FROM_D
	,DT2.VALD_TO_D  
  ,DT2.ROLE_PLAY_TYPE_X              
	,DT2.DERV_PRTF_ROLE_C              
	,DT2.SRCE_SYST_C    
	,DT2.ROW_SECU_ACCS_C               
	,DT2.PROS_KEY_I                    
From
  (
		Select Distinct
			 C.INT_GRUP_I                    
			,C.ROLE_PLAY_I                   
			,C.EFFT_D                        
			,C.EXPY_D   
			,C.VALD_FROM_D
			,C.VALD_TO_D  
			,C.PERD_D                        
			,C.ROLE_PLAY_TYPE_X              
			,C.DERV_PRTF_ROLE_C              
			,C.SRCE_SYST_C     
			,C.ROW_N
			,C.ROW_SECU_ACCS_C               
			,C.PROS_KEY_I                    
      ,MAX(Coalesce( DT1.ROW_N, 0 )) Over (Partition By C.INT_GRUP_I, C.ROLE_PLAY_I, C.PERD_D) as GRUP_N 	
		From
			%%VTECH%%.DERV_PRTF_INT_GRUP_OWN_PSST C
			Left Join (
				-- Detect the change in non-key values between rows
				Select
           A.INT_GRUP_I
          ,A.ROLE_PLAY_I
          ,A.DERV_PRTF_ROLE_C
          ,A.ROLE_PLAY_TYPE_X
          ,A.ROW_N
          ,A.PERD_D
          ,A.EFFT_D
				From
					%%VTECH%%.DERV_PRTF_INT_GRUP_OWN_PSST A
					Inner Join %%VTECH%%.DERV_PRTF_INT_GRUP_OWN_PSST B
					
					On A.INT_GRUP_I = B.INT_GRUP_I
					And A.ROW_N = B.ROW_N + 1					
					And A.DERV_PRTF_ROLE_C = B.DERV_PRTF_ROLE_C					
          And A.ROLE_PLAY_TYPE_X = B.ROLE_PLAY_TYPE_X

          -- Detect the change
          And ( 
            A.ROLE_PLAY_TYPE_X <> B.ROLE_PLAY_TYPE_X
            Or A.ROLE_PLAY_I <> B.ROLE_PLAY_I
  					Or  A.EFFT_D <> B.EFFT_D
					)

			) DT1
			On C.INT_GRUP_I = DT1.INT_GRUP_I
			And C.ROLE_PLAY_TYPE_X = DT1.ROLE_PLAY_TYPE_X
			And C.EFFT_D = DT1.EFFT_D
      And C.ROW_N >= DT1.ROW_N
      And DT1.PERD_D <= C.PERD_D 

  ) DT2
;
```

#### ❄️ Converted Snowflake SQL:
```sql
INSERT INTO %%STARDATADB%%.DERV_PRTF_OWN_HIST_PSST SELECT DISTINCT DT2.INT_GRUP_I, DT2.ROLE_PLAY_I, MIN(DT2.PERD_D) OVER (PARTITION BY DT2.INT_GRUP_I, DT2.ROLE_PLAY_I, DT2.GRUP_N) AS JOIN_FROM_D, MAX(DT2.PERD_D) OVER (PARTITION BY DT2.INT_GRUP_I, DT2.ROLE_PLAY_I, DT2.GRUP_N) AS JOIN_TO_D, DT2.EFFT_D, DT2.EXPY_D, DT2.VALD_FROM_D, DT2.VALD_TO_D, DT2.ROLE_PLAY_TYPE_X, DT2.DERV_PRTF_ROLE_C, DT2.SRCE_SYST_C, DT2.ROW_SECU_ACCS_C, DT2.PROS_KEY_I FROM (SELECT DISTINCT C.INT_GRUP_I, C.ROLE_PLAY_I, C.EFFT_D, C.EXPY_D, C.VALD_FROM_D, C.VALD_TO_D, C.PERD_D, C.ROLE_PLAY_TYPE_X, C.DERV_PRTF_ROLE_C, C.SRCE_SYST_C, C.ROW_N, C.ROW_SECU_ACCS_C, C.PROS_KEY_I, MAX(COALESCE(DT1.ROW_N, 0)) OVER (PARTITION BY C.INT_GRUP_I, C.ROLE_PLAY_I, C.PERD_D) AS GRUP_N FROM %%VTECH%%.DERV_PRTF_INT_GRUP_OWN_PSST AS C LEFT JOIN (SELECT A.INT_GRUP_I, A.ROLE_PLAY_I, A.DERV_PRTF_ROLE_C, A.ROLE_PLAY_TYPE_X, A.ROW_N, A.PERD_D, A.EFFT_D FROM %%VTECH%%.DERV_PRTF_INT_GRUP_OWN_PSST AS A INNER JOIN %%VTECH%%.DERV_PRTF_INT_GRUP_OWN_PSST AS B ON A.INT_GRUP_I = B.INT_GRUP_I AND A.ROW_N = B.ROW_N + 1 AND A.DERV_PRTF_ROLE_C = B.DERV_PRTF_ROLE_C AND A.ROLE_PLAY_TYPE_X = B.ROLE_PLAY_TYPE_X AND (A.ROLE_PLAY_TYPE_X <> B.ROLE_PLAY_TYPE_X OR A.ROLE_PLAY_I <> B.ROLE_PLAY_I OR A.EFFT_D <> B.EFFT_D)) AS DT1 ON C.INT_GRUP_I = DT1.INT_GRUP_I AND C.ROLE_PLAY_TYPE_X = DT1.ROLE_PLAY_TYPE_X AND C.EFFT_D = DT1.EFFT_D AND C.ROW_N >= DT1.ROW_N AND DT1.PERD_D <= C.PERD_D) AS DT2
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
INSERT INTO "%%stardatadb%%"."derv_prtf_own_hist_psst"
WITH "dt2" AS (
  SELECT DISTINCT
    "c"."int_grup_i" AS "int_grup_i",
    "c"."role_play_i" AS "role_play_i",
    "c"."efft_d" AS "efft_d",
    "c"."expy_d" AS "expy_d",
    "c"."vald_from_d" AS "vald_from_d",
    "c"."vald_to_d" AS "vald_to_d",
    "c"."perd_d" AS "perd_d",
    "c"."role_play_type_x" AS "role_play_type_x",
    "c"."derv_prtf_role_c" AS "derv_prtf_role_c",
    "c"."srce_syst_c" AS "srce_syst_c",
    "c"."row_n" AS "row_n",
    "c"."row_secu_accs_c" AS "row_secu_accs_c",
    "c"."pros_key_i" AS "pros_key_i",
    MAX(COALESCE("a"."row_n", 0)) OVER (PARTITION BY "c"."int_grup_i", "c"."role_play_i", "c"."perd_d") AS "grup_n"
  FROM "%%vtech%%"."derv_prtf_int_grup_own_psst" AS "c"
  LEFT JOIN "%%vtech%%"."derv_prtf_int_grup_own_psst" AS "a"
    ON "a"."efft_d" = "c"."efft_d"
    AND "a"."int_grup_i" = "c"."int_grup_i"
    AND "a"."perd_d" <= "c"."perd_d"
    AND "a"."role_play_type_x" = "c"."role_play_type_x"
    AND "a"."row_n" <= "c"."row_n"
  JOIN "%%vtech%%"."derv_prtf_int_grup_own_psst" AS "b"
    ON "a"."derv_prtf_role_c" = "b"."derv_prtf_role_c"
    AND (
      "a"."efft_d" <> "b"."efft_d"
      OR "a"."role_play_i" <> "b"."role_play_i"
      OR "a"."role_play_type_x" <> "b"."role_play_type_x"
    )
    AND "a"."int_grup_i" = "b"."int_grup_i"
    AND "a"."role_play_type_x" = "b"."role_play_type_x"
    AND "a"."row_n" = "b"."row_n" + 1
)
SELECT DISTINCT
  "dt2"."int_grup_i" AS "int_grup_i",
  "dt2"."role_play_i" AS "role_play_i",
  MIN("dt2"."perd_d") OVER (PARTITION BY "dt2"."int_grup_i", "dt2"."role_play_i", "dt2"."grup_n") AS "join_from_d",
  MAX("dt2"."perd_d") OVER (PARTITION BY "dt2"."int_grup_i", "dt2"."role_play_i", "dt2"."grup_n") AS "join_to_d",
  "dt2"."efft_d" AS "efft_d",
  "dt2"."expy_d" AS "expy_d",
  "dt2"."vald_from_d" AS "vald_from_d",
  "dt2"."vald_to_d" AS "vald_to_d",
  "dt2"."role_play_type_x" AS "role_play_type_x",
  "dt2"."derv_prtf_role_c" AS "derv_prtf_role_c",
  "dt2"."srce_syst_c" AS "srce_syst_c",
  "dt2"."row_secu_accs_c" AS "row_secu_accs_c",
  "dt2"."pros_key_i" AS "pros_key_i"
FROM "dt2" AS "dt2"
```

#### 📊 SQL Metadata:
- **Tables**: DERV_PRTF_INT_GRUP_OWN_PSST, DERV_PRTF_OWN_HIST_PSST
- **Columns**: DERV_PRTF_ROLE_C, EFFT_D, EXPY_D, GRUP_N, INT_GRUP_I, PERD_D, PROS_KEY_I, ROLE_PLAY_I, ROLE_PLAY_TYPE_X, ROW_N, ROW_SECU_ACCS_C, SRCE_SYST_C, VALD_FROM_D, VALD_TO_D
- **Functions**: A.INT_GRUP_I = B.INT_GRUP_I, A.INT_GRUP_I = B.INT_GRUP_I AND A.ROW_N = B.ROW_N + 1, A.INT_GRUP_I = B.INT_GRUP_I AND A.ROW_N = B.ROW_N + 1 AND A.DERV_PRTF_ROLE_C = B.DERV_PRTF_ROLE_C, A.INT_GRUP_I = B.INT_GRUP_I AND A.ROW_N = B.ROW_N + 1 AND A.DERV_PRTF_ROLE_C = B.DERV_PRTF_ROLE_C AND A.ROLE_PLAY_TYPE_X = B.ROLE_PLAY_TYPE_X, A.ROLE_PLAY_TYPE_X <> B.ROLE_PLAY_TYPE_X, A.ROLE_PLAY_TYPE_X <> B.ROLE_PLAY_TYPE_X OR A.ROLE_PLAY_I <> B.ROLE_PLAY_I, C.INT_GRUP_I = DT1.INT_GRUP_I, C.INT_GRUP_I = DT1.INT_GRUP_I AND C.ROLE_PLAY_TYPE_X = DT1.ROLE_PLAY_TYPE_X, C.INT_GRUP_I = DT1.INT_GRUP_I AND C.ROLE_PLAY_TYPE_X = DT1.ROLE_PLAY_TYPE_X AND C.EFFT_D = DT1.EFFT_D, C.INT_GRUP_I = DT1.INT_GRUP_I AND C.ROLE_PLAY_TYPE_X = DT1.ROLE_PLAY_TYPE_X AND C.EFFT_D = DT1.EFFT_D AND C.ROW_N >= DT1.ROW_N, COALESCE(DT1.ROW_N, 0), DT1.ROW_N, DT2.PERD_D
- **Window_Functions**: COALESCE(DT1.ROW_N, 0), DT2.PERD_D

### SQL Block 11 (Lines 364-373)
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
	 %%STARDATADB%%.DERV_PRTF_OWN_PSST A
	,%%VTECH%%.DERV_PRTF_OWN_HIST_PSST B
Where  
  A.INT_GRUP_I = B.INT_GRUP_I
  And A.DERV_PRTF_ROLE_C = B.DERV_PRTF_ROLE_C
  And (A.JOIN_FROM_D,A.JOIN_TO_D) overlaps (B.JOIN_FROM_D,B.JOIN_TO_D)     
;
```

#### ❄️ Converted Snowflake SQL:
```sql
DELETE A FROM %%STARDATADB%%.DERV_PRTF_OWN_PSST AS A, %%VTECH%%.DERV_PRTF_OWN_HIST_PSST AS B WHERE A.INT_GRUP_I = B.INT_GRUP_I AND A.DERV_PRTF_ROLE_C = B.DERV_PRTF_ROLE_C AND (A.JOIN_FROM_D, A.JOIN_TO_D) OVERLAPS (B.JOIN_FROM_D, B.JOIN_TO_D)
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
DELETE   "a" FROM "%%stardatadb%%"."derv_prtf_own_psst" AS "a"
  CROSS JOIN "%%vtech%%"."derv_prtf_own_hist_psst" AS "b"
WHERE
  "a"."derv_prtf_role_c" = "b"."derv_prtf_role_c"
  AND "a"."int_grup_i" = "b"."int_grup_i"
  AND ("a"."join_from_d", "a"."join_to_d") OVERLAPS ("b"."join_from_d", "b"."join_to_d")
```

#### 📊 SQL Metadata:
- **Tables**: A, DERV_PRTF_OWN_HIST_PSST, DERV_PRTF_OWN_PSST
- **Columns**: DERV_PRTF_ROLE_C, INT_GRUP_I, JOIN_FROM_D, JOIN_TO_D
- **Functions**: A.INT_GRUP_I = B.INT_GRUP_I, A.INT_GRUP_I = B.INT_GRUP_I AND A.DERV_PRTF_ROLE_C = B.DERV_PRTF_ROLE_C

### SQL Block 12 (Lines 376-397)
- **Complexity Score**: 0
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ❌ Invalid
- **Teradata Features**: 2

#### 📝 Original Teradata SQL:
```sql
Insert into %%STARDATADB%%.DERV_PRTF_OWN_PSST
Select
   INT_GRUP_I                    
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
  ,DERV_PRTF_ROLE_C              
  ,ROLE_PLAY_TYPE_X              
  ,ROLE_PLAY_I                   
  ,SRCE_SYST_C                   
  ,ROW_SECU_ACCS_C               
  ,PROS_KEY_I                                           
From 
  %%VTECH%%.DERV_PRTF_OWN_HIST_PSST
;
```

#### ❄️ Converted Snowflake SQL:
```sql
Insert into %%STARDATADB%%.DERV_PRTF_OWN_PSST
Select
   INT_GRUP_I                    
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
  ,DERV_PRTF_ROLE_C              
  ,ROLE_PLAY_TYPE_X              
  ,ROLE_PLAY_I                   
  ,SRCE_SYST_C                   
  ,ROW_SECU_ACCS_C               
  ,PROS_KEY_I                                           
From 
  %%VTECH%%.DERV_PRTF_OWN_HIST_PSST
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

*Generated by BTEQ Parser Service - 2025-08-20 12:39:18*
