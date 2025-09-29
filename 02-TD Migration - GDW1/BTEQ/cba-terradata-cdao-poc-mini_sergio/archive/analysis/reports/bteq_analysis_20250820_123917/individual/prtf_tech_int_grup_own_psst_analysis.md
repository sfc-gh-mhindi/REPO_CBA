# prtf_tech_int_grup_own_psst.sql - BTEQ Analysis

## File Overview
- **File Name**: prtf_tech_int_grup_own_psst.sql
- **Analysis Status**: ✅ Success
- **Control Statements**: 16
- **SQL Blocks**: 8

## Control Flow Analysis

| Line | Type | Statement |
|------|------|-----------|
| 1 | RUN | `.RUN FILE=%%BTEQ_LOGON_SCRIPT%%` |
| 2 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 36 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 106 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 110 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 118 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 178 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 181 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 189 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 275 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 278 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 296 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 320 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 323 | LOGOFF | `.LOGOFF` |
| 325 | LABEL | `.LABEL EXITERR` |
| 327 | LOGOFF | `.LOGOFF` |

## SQL Blocks Analysis

### SQL Block 1 (Lines 34-35)
- **Complexity Score**: 6
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
Delete from %%STARDATADB%%.DERV_PRTF_OWN_PSST All
;
```

#### ❄️ Converted Snowflake SQL:
```sql
DELETE FROM %%STARDATADB%%.DERV_PRTF_OWN_PSST AS All
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
DELETE FROM "%%stardatadb%%"."derv_prtf_own_psst" AS "all"
```

#### 📊 SQL Metadata:
- **Tables**: DERV_PRTF_OWN_PSST

### SQL Block 2 (Lines 38-105)
- **Complexity Score**: 0
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ❌ Invalid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
Insert into %%STARDATADB%%.DERV_PRTF_OWN_PSST
Select
   IGE.INT_GRUP_I                    
  ,IGE.VALD_FROM_D as JOIN_FROM_D                   
  ,Coalesce(IGE.VALD_TO_D, ('9999-12-31' (Date,format'YYYY-MM-DD')) ) as JOIN_TO_D
  ,IGE.VALD_FROM_D                   
  ,IGE.VALD_TO_D
  ,IGE.EFFT_D                        
  ,IGE.EXPY_D                                            
   ,(Case
      When  IGE.EMPL_ROLE_C = 'OWN' Then 'OWNR'
      When  IGE.EMPL_ROLE_C = 'AOW' Then 'AOWN'
      When  IGE.EMPL_ROLE_C = 'AST' Then 'ASTT'      
      Else Null
     End)                  as DERV_PRTF_ROLE_C	               
  ,('Employee'(VARCHAR(40))) as ROLE_PLAY_TYPE_X  
  ,IGE.EMPL_I as ROLE_PLAY_I                        
  ,IGE.SRCE_SYST_C                                       
  ,IGE.ROW_SECU_ACCS_C               
  ,IGE.PROS_KEY_EFFT_I               
From
  %%VTECH%%.INT_GRUP_EMPL IGE

 /* Add the GRD filter to reduce the data */
  INNER JOIN %%VTECH%%.INT_GRUP IG
  ON IG.INT_GRUP_I = IGE.INT_GRUP_I
    
  INNER JOIN %%VTECH%%.GRD_PRTF_TYPE_ENHC_HIST_PSST GPTE  
  ON GPTE.PRTF_TYPE_C = IG.INT_GRUP_TYPE_C
  AND (GPTE.VALD_FROM_D,GPTE.VALD_TO_D) Overlaps (IG.CRAT_D,IG.VALD_TO_D)
  AND (GPTE.VALD_FROM_D,GPTE.VALD_TO_D) Overlaps (IGE.VALD_FROM_D,IGE.VALD_TO_D)
Group By 1,2,3,4,5,6,7,8,9,10,11,12,13


Union All

Select
   IGD.INT_GRUP_I                                   
  ,IGD.VALD_FROM_D as JOIN_FROM_D                   
  ,Coalesce(IGD.VALD_TO_D, ('9999-12-31' (Date,format'YYYY-MM-DD')) ) as JOIN_TO_D              
  ,IGD.VALD_FROM_D                 
  ,IGD.VALD_TO_D                  
  ,IGD.EFFT_D                        
  ,IGD.EXPY_D                        
  ,(Case
      When IGD.DEPT_ROLE_C = 'OWNG' Then 'OWNR'
      When IGD.DEPT_ROLE_C = 'STDP' Then 'STDP'   
      Else Null
    End)                    as DERV_PRTF_ROLE_C                
  ,('Department'(VARCHAR(40))) as ROLE_PLAY_TYPE_X              
  ,IGD.DEPT_I as ROLE_PLAY_I                   
  ,IGD.SRCE_SYST_C                   
  ,IGD.ROW_SECU_ACCS_C       
  ,IGD.PROS_KEY_EFFT_I       
from
%%VTECH%%.INT_GRUP_DEPT IGD

 /* Add the GRD filter to reduce the data */
  INNER JOIN %%VTECH%%.INT_GRUP IG
  ON IG.INT_GRUP_I = IGD.INT_GRUP_I
    
  INNER JOIN %%VTECH%%.GRD_PRTF_TYPE_ENHC_HIST_PSST GPTE  
  ON GPTE.PRTF_TYPE_C = IG.INT_GRUP_TYPE_C
  AND (GPTE.VALD_FROM_D,GPTE.VALD_TO_D) Overlaps (IG.CRAT_D,IG.VALD_TO_D)
  AND (GPTE.VALD_FROM_D,GPTE.VALD_TO_D) Overlaps (IGD.VALD_FROM_D,IGD.VALD_TO_D)

Group By 1,2,3,4,5,6,7,8,9,10,11,12,13
;
```

#### ❄️ Converted Snowflake SQL:
```sql
Insert into %%STARDATADB%%.DERV_PRTF_OWN_PSST
Select
   IGE.INT_GRUP_I                    
  ,IGE.VALD_FROM_D as JOIN_FROM_D                   
  ,Coalesce(IGE.VALD_TO_D, ('9999-12-31' (Date,format'YYYY-MM-DD')) ) as JOIN_TO_D
  ,IGE.VALD_FROM_D                   
  ,IGE.VALD_TO_D
  ,IGE.EFFT_D                        
  ,IGE.EXPY_D                                            
   ,(Case
      When  IGE.EMPL_ROLE_C = 'OWN' Then 'OWNR'
      When  IGE.EMPL_ROLE_C = 'AOW' Then 'AOWN'
      When  IGE.EMPL_ROLE_C = 'AST' Then 'ASTT'      
      Else Null
     End)                  as DERV_PRTF_ROLE_C	               
  ,('Employee'(VARCHAR(40))) as ROLE_PLAY_TYPE_X  
  ,IGE.EMPL_I as ROLE_PLAY_I                        
  ,IGE.SRCE_SYST_C                                       
  ,IGE.ROW_SECU_ACCS_C               
  ,IGE.PROS_KEY_EFFT_I               
From
  %%VTECH%%.INT_GRUP_EMPL IGE

 /* Add the GRD filter to reduce the data */
  INNER JOIN %%VTECH%%.INT_GRUP IG
  ON IG.INT_GRUP_I = IGE.INT_GRUP_I
    
  INNER JOIN %%VTECH%%.GRD_PRTF_TYPE_ENHC_HIST_PSST GPTE  
  ON GPTE.PRTF_TYPE_C = IG.INT_GRUP_TYPE_C
  AND (GPTE.VALD_FROM_D,GPTE.VALD_TO_D) Overlaps (IG.CRAT_D,IG.VALD_TO_D)
  AND (GPTE.VALD_FROM_D,GPTE.VALD_TO_D) Overlaps (IGE.VALD_FROM_D,IGE.VALD_TO_D)
Group By 1,2,3,4,5,6,7,8,9,10,11,12,13


Union All

Select
   IGD.INT_GRUP_I                                   
  ,IGD.VALD_FROM_D as JOIN_FROM_D                   
  ,Coalesce(IGD.VALD_TO_D, ('9999-12-31' (Date,format'YYYY-MM-DD')) ) as JOIN_TO_D              
  ,IGD.VALD_FROM_D                 
  ,IGD.VALD_TO_D                  
  ,IGD.EFFT_D                        
  ,IGD.EXPY_D                        
  ,(Case
      When IGD.DEPT_ROLE_C = 'OWNG' Then 'OWNR'
      When IGD.DEPT_ROLE_C = 'STDP' Then 'STDP'   
      Else Null
    End)                    as DERV_PRTF_ROLE_C                
  ,('Department'(VARCHAR(40))) as ROLE_PLAY_TYPE_X              
  ,IGD.DEPT_I as ROLE_PLAY_I                   
  ,IGD.SRCE_SYST_C                   
  ,IGD.ROW_SECU_ACCS_C       
  ,IGD.PROS_KEY_EFFT_I       
from
%%VTECH%%.INT_GRUP_DEPT IGD

 /* Add the GRD filter to reduce the data */
  INNER JOIN %%VTECH%%.INT_GRUP IG
  ON IG.INT_GRUP_I = IGD.INT_GRUP_I
    
  INNER JOIN %%VTECH%%.GRD_PRTF_TYPE_ENHC_HIST_PSST GPTE  
  ON GPTE.PRTF_TYPE_C = IG.INT_GRUP_TYPE_C
  AND (GPTE.VALD_FROM_D,GPTE.VALD_TO_D) Overlaps (IG.CRAT_D,IG.VALD_TO_D)
  AND (GPTE.VALD_FROM_D,GPTE.VALD_TO_D) Overlaps (IGD.VALD_FROM_D,IGD.VALD_TO_D)

Group By 1,2,3,4,5,6,7,8,9,10,11,12,13
;
```

#### 🔍 Syntax Validation Details:
- **Valid**: ❌

#### 🎯 Teradata Features Detected:
- Variable substitution

### SQL Block 3 (Lines 116-117)
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

### SQL Block 4 (Lines 120-177)
- **Complexity Score**: 280
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
	,ROW_NUMBER() OVER (Partition By DT2.INT_GRUP_I, DT2.ROLE_PLAY_I Order by DT2.PERD_D, DT2.DERV_PRTF_ROLE_C )
  ,DT2.ROW_SECU_ACCS_C      as ROW_SECU_ACCS_C
  ,0                        as PROS_KEY_I
From
	(
    select DERV.*,C.CALENDAR_DATE as PERD_D  from 
    (Select
       A.INT_GRUP_I           as INT_GRUP_I
      ,A.ROLE_PLAY_I
      ,A.DERV_PRTF_ROLE_C  
      --,C.CALENDAR_DATE as PERD_D   
      ,A.EFFT_D               as EFFT_D
      ,A.EXPY_D               as EXPY_D
      ,A.ROLE_PLAY_TYPE_X 
      ,A.VALD_FROM_D
      ,A.VALD_TO_D
      ,A.SRCE_SYST_C          as SRCE_SYST_C
      ,A.ROW_SECU_ACCS_C      as ROW_SECU_ACCS_C
	  ,A.JOIN_FROM_D
	  ,A.JOIN_TO_D
    From
      /* KEY = (INT_GRUP_I, ROLE_PLAY_I) */
      %%VTECH%%.DERV_PRTF_OWN_PSST A
      Inner Join %%VTECH%%.DERV_PRTF_OWN_PSST  B
      On A.INT_GRUP_I = B.INT_GRUP_I
      And A.ROLE_PLAY_I = B.ROLE_PLAY_I
      And A.ROLE_PLAY_TYPE_X = B.ROLE_PLAY_TYPE_X
      And A.DERV_PRTF_ROLE_C = B.DERV_PRTF_ROLE_C
      And A.DERV_PRTF_ROLE_C = 'OWNR' -- Can have multiple Assistant and other role codes
      And (A.JOIN_FROM_D,A.JOIN_TO_D) overlaps (B.JOIN_FROM_D,B.JOIN_TO_D)   
	  --AND A.INT_GRUP_I<> 'SAPPF2479S96'
	  group by 1,2,3,4,5,6,7,8,9,10,11,12)DERV
      --**** Process all overlap records
      --And (
      --	A.JOIN_FROM_D <> B.JOIN_FROM_D
      --	Or A.JOIN_TO_D <> B.JOIN_TO_D
      --  Or ROLE_PLAY_I <> B.ROLE_PLAY_I
      --)
	   
      Inner Join %%VTECH%%.CALENDAR C
      On C.CALENDAR_DATE between DERV.JOIN_FROM_D and DERV.JOIN_TO_D
      And C.CALENDAR_DATE between ADD_MONTHS((Current_Date - EXTRACT(DAY from CURRENT_DATE) +1 ),-39) and ADD_MONTHS(Current_Date, 1)
    --Qualify Row_Number() Over( Partition By A.INT_GRUP_I, A.ROLE_PLAY_I, C.CALENDAR_DATE Order BY A.EFFT_D Desc ) = 1
    Qualify Row_Number() Over( Partition By DERV.INT_GRUP_I, DERV.ROLE_PLAY_TYPE_X, C.CALENDAR_DATE Order BY DERV.EFFT_D Desc,  DERV.ROLE_PLAY_I ) = 1 
	) DT2
;
```

#### ❄️ Converted Snowflake SQL:
```sql
INSERT INTO %%STARDATADB%%.DERV_PRTF_INT_GRUP_OWN_PSST SELECT DT2.INT_GRUP_I AS INT_GRUP_I, DT2.ROLE_PLAY_I AS ROLE_PLAY_I, DT2.EFFT_D AS EFFT_D, DT2.EXPY_D AS EXPY_D, DT2.VALD_FROM_D AS VALD_FROM_D, DT2.VALD_TO_D AS VALD_TO_D, DT2.PERD_D AS PERD_D, DT2.ROLE_PLAY_TYPE_X AS ROLE_PLAY_TYPE_X, DT2.DERV_PRTF_ROLE_C AS DERV_PRTF_ROLE_C, DT2.SRCE_SYST_C AS SRCE_SYST_C, ROW_NUMBER() OVER (PARTITION BY DT2.INT_GRUP_I, DT2.ROLE_PLAY_I ORDER BY DT2.PERD_D NULLS FIRST, DT2.DERV_PRTF_ROLE_C NULLS FIRST), DT2.ROW_SECU_ACCS_C AS ROW_SECU_ACCS_C, 0 AS PROS_KEY_I FROM (SELECT DERV.*, C.CALENDAR_DATE AS PERD_D FROM (SELECT A.INT_GRUP_I AS INT_GRUP_I, A.ROLE_PLAY_I, A.DERV_PRTF_ROLE_C, A.EFFT_D AS EFFT_D, A.EXPY_D AS EXPY_D, A.ROLE_PLAY_TYPE_X, A.VALD_FROM_D, A.VALD_TO_D, A.SRCE_SYST_C AS SRCE_SYST_C, A.ROW_SECU_ACCS_C AS ROW_SECU_ACCS_C, A.JOIN_FROM_D, A.JOIN_TO_D FROM %%VTECH%%.DERV_PRTF_OWN_PSST AS A INNER JOIN %%VTECH%%.DERV_PRTF_OWN_PSST AS B ON A.INT_GRUP_I = B.INT_GRUP_I AND A.ROLE_PLAY_I = B.ROLE_PLAY_I AND A.ROLE_PLAY_TYPE_X = B.ROLE_PLAY_TYPE_X AND A.DERV_PRTF_ROLE_C = B.DERV_PRTF_ROLE_C AND A.DERV_PRTF_ROLE_C = 'OWNR' AND (A.JOIN_FROM_D, A.JOIN_TO_D) OVERLAPS (B.JOIN_FROM_D, B.JOIN_TO_D) GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12) AS DERV INNER JOIN %%VTECH%%.CALENDAR AS C ON C.CALENDAR_DATE BETWEEN DERV.JOIN_FROM_D AND DERV.JOIN_TO_D AND C.CALENDAR_DATE BETWEEN ADD_MONTHS((CURRENT_DATE - DATE_PART(DAY, CURRENT_DATE) + 1), -39) AND ADD_MONTHS(CURRENT_DATE, 1) QUALIFY ROW_NUMBER() OVER (PARTITION BY DERV.INT_GRUP_I, DERV.ROLE_PLAY_TYPE_X, C.CALENDAR_DATE ORDER BY DERV.EFFT_D DESC NULLS LAST, DERV.ROLE_PLAY_I NULLS FIRST) = 1) AS DT2
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
WITH "derv" AS (
  SELECT
    "a"."int_grup_i" AS "int_grup_i",
    "a"."role_play_i" AS "role_play_i",
    "a"."derv_prtf_role_c" AS "derv_prtf_role_c",
    "a"."efft_d" AS "efft_d",
    "a"."expy_d" AS "expy_d",
    "a"."role_play_type_x" AS "role_play_type_x",
    "a"."vald_from_d" AS "vald_from_d",
    "a"."vald_to_d" AS "vald_to_d",
    "a"."srce_syst_c" AS "srce_syst_c",
    "a"."row_secu_accs_c" AS "row_secu_accs_c",
    "a"."join_from_d" AS "join_from_d",
    "a"."join_to_d" AS "join_to_d"
  FROM "%%vtech%%"."derv_prtf_own_psst" AS "a"
  JOIN "%%vtech%%"."derv_prtf_own_psst" AS "b"
    ON "a"."derv_prtf_role_c" = "b"."derv_prtf_role_c"
    AND "a"."derv_prtf_role_c" = 'OWNR'
    AND "a"."int_grup_i" = "b"."int_grup_i"
    AND "a"."role_play_i" = "b"."role_play_i"
    AND "a"."role_play_type_x" = "b"."role_play_type_x"
    AND ("a"."join_from_d", "a"."join_to_d") OVERLAPS ("b"."join_from_d", "b"."join_to_d")
  GROUP BY
    "a"."int_grup_i",
    "a"."role_play_i",
    "a"."derv_prtf_role_c",
    "a"."efft_d",
    "a"."expy_d",
    "a"."role_play_type_x",
    "a"."vald_from_d",
    "a"."vald_to_d",
    "a"."srce_syst_c",
    "a"."row_secu_accs_c",
    "a"."join_from_d",
    "a"."join_to_d"
), "dt2" AS (
  SELECT
    "derv"."int_grup_i" AS "int_grup_i",
    "derv"."role_play_i" AS "role_play_i",
    "derv"."derv_prtf_role_c" AS "derv_prtf_role_c",
    "derv"."efft_d" AS "efft_d",
    "derv"."expy_d" AS "expy_d",
    "derv"."role_play_type_x" AS "role_play_type_x",
    "derv"."vald_from_d" AS "vald_from_d",
    "derv"."vald_to_d" AS "vald_to_d",
    "derv"."srce_syst_c" AS "srce_syst_c",
    "derv"."row_secu_accs_c" AS "row_secu_accs_c",
    "c"."calendar_date" AS "perd_d"
  FROM "derv" AS "derv"
  JOIN "%%vtech%%"."calendar" AS "c"
    ON "c"."calendar_date" <= "derv"."join_to_d"
    AND "c"."calendar_date" <= ADD_MONTHS(CURRENT_DATE, 1)
    AND "c"."calendar_date" >= "derv"."join_from_d"
    AND "c"."calendar_date" >= ADD_MONTHS((
      CURRENT_DATE - DATE_PART(DAY, CURRENT_DATE) + 1
    ), -39)
  QUALIFY
    ROW_NUMBER() OVER (
      PARTITION BY "derv"."int_grup_i", "derv"."role_play_type_x", "c"."calendar_date"
      ORDER BY "derv"."efft_d" DESC NULLS LAST, "derv"."role_play_i" NULLS FIRST
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
    PARTITION BY "dt2"."int_grup_i", "dt2"."role_play_i"
    ORDER BY "dt2"."perd_d" NULLS FIRST, "dt2"."derv_prtf_role_c" NULLS FIRST
  ) AS "_col_10",
  "dt2"."row_secu_accs_c" AS "row_secu_accs_c",
  0 AS "pros_key_i"
FROM "dt2" AS "dt2"
```

#### 📊 SQL Metadata:
- **Tables**: CALENDAR, DERV_PRTF_INT_GRUP_OWN_PSST, DERV_PRTF_OWN_PSST
- **Columns**: *, CALENDAR_DATE, DERV_PRTF_ROLE_C, EFFT_D, EXPY_D, INT_GRUP_I, JOIN_FROM_D, JOIN_TO_D, PERD_D, ROLE_PLAY_I, ROLE_PLAY_TYPE_X, ROW_SECU_ACCS_C, SRCE_SYST_C, VALD_FROM_D, VALD_TO_D
- **Functions**: (CURRENT_DATE - EXTRACT(DAY FROM CURRENT_DATE) + 1), A.INT_GRUP_I = B.INT_GRUP_I, A.INT_GRUP_I = B.INT_GRUP_I AND A.ROLE_PLAY_I = B.ROLE_PLAY_I, A.INT_GRUP_I = B.INT_GRUP_I AND A.ROLE_PLAY_I = B.ROLE_PLAY_I AND A.ROLE_PLAY_TYPE_X = B.ROLE_PLAY_TYPE_X, A.INT_GRUP_I = B.INT_GRUP_I AND A.ROLE_PLAY_I = B.ROLE_PLAY_I AND A.ROLE_PLAY_TYPE_X = B.ROLE_PLAY_TYPE_X AND A.DERV_PRTF_ROLE_C = B.DERV_PRTF_ROLE_C, A.INT_GRUP_I = B.INT_GRUP_I AND A.ROLE_PLAY_I = B.ROLE_PLAY_I AND A.ROLE_PLAY_TYPE_X = B.ROLE_PLAY_TYPE_X AND A.DERV_PRTF_ROLE_C = B.DERV_PRTF_ROLE_C AND A.DERV_PRTF_ROLE_C = 'OWNR', C.CALENDAR_DATE BETWEEN DERV.JOIN_FROM_D AND DERV.JOIN_TO_D, CURRENT_DATE, DAY, None
- **Window_Functions**: None

### SQL Block 5 (Lines 187-188)
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

### SQL Block 6 (Lines 191-274)
- **Complexity Score**: 0
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ❌ Invalid
- **Teradata Features**: 2

#### 📝 Original Teradata SQL:
```sql
Insert into %%STARDATADB%%.DERV_PRTF_OWN_HIST_PSST 
Select
	 DT3.INT_GRUP_I                    
	,DT3.ROLE_PLAY_I 
  ,DT3.JOIN_FROM_D
  ,(Case
      WHEN DT3.JOIN_TO_D = ADD_MONTHS(CURRENT_DATE, 1) Then ('9999-12-31'(Date,format'yyyy-mm-dd'))             
      ELSE DT3.JOIN_TO_D
    End
    ) as JOIN_TO_D 
	,DT3.EFFT_D                        
	,DT3.EXPY_D   
	,DT3.VALD_FROM_D
	,DT3.VALD_TO_D  
	,DT3.ROLE_PLAY_TYPE_X              
	,DT3.DERV_PRTF_ROLE_C              
	,DT3.SRCE_SYST_C     
	,DT3.ROW_SECU_ACCS_C               
	,DT3.PROS_KEY_I    
From
  (
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
        Select
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
              ,A.ROW_N
              ,A.EFFT_D
              ,A.PERD_D
            From
              %%VTECH%%.DERV_PRTF_INT_GRUP_OWN_PSST A
              Inner Join %%VTECH%%.DERV_PRTF_INT_GRUP_OWN_PSST B
              On A.INT_GRUP_I = B.INT_GRUP_I
              And A.ROLE_PLAY_I = B.ROLE_PLAY_I
              And A.ROW_N = B.ROW_N + 1
              And (
                A.DERV_PRTF_ROLE_C <> B.DERV_PRTF_ROLE_C
                Or
                A.EFFT_D <> B.EFFT_D
              )
          ) DT1
          On C.INT_GRUP_I = DT1.INT_GRUP_I
          And C.ROLE_PLAY_I = DT1.ROLE_PLAY_I
          And C.EFFT_D = DT1.EFFT_D
          And C.ROW_N >= DT1.ROW_N
          And DT1.PERD_D <= C.PERD_D 

      ) DT2
  ) DT3
;
```

#### ❄️ Converted Snowflake SQL:
```sql
Insert into %%STARDATADB%%.DERV_PRTF_OWN_HIST_PSST 
Select
	 DT3.INT_GRUP_I                    
	,DT3.ROLE_PLAY_I 
  ,DT3.JOIN_FROM_D
  ,(Case
      WHEN DT3.JOIN_TO_D = ADD_MONTHS(CURRENT_DATE, 1) Then ('9999-12-31'(Date,format'yyyy-mm-dd'))             
      ELSE DT3.JOIN_TO_D
    End
    ) as JOIN_TO_D 
	,DT3.EFFT_D                        
	,DT3.EXPY_D   
	,DT3.VALD_FROM_D
	,DT3.VALD_TO_D  
	,DT3.ROLE_PLAY_TYPE_X              
	,DT3.DERV_PRTF_ROLE_C              
	,DT3.SRCE_SYST_C     
	,DT3.ROW_SECU_ACCS_C               
	,DT3.PROS_KEY_I    
From
  (
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
        Select
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
              ,A.ROW_N
              ,A.EFFT_D
              ,A.PERD_D
            From
              %%VTECH%%.DERV_PRTF_INT_GRUP_OWN_PSST A
              Inner Join %%VTECH%%.DERV_PRTF_INT_GRUP_OWN_PSST B
              On A.INT_GRUP_I = B.INT_GRUP_I
              And A.ROLE_PLAY_I = B.ROLE_PLAY_I
              And A.ROW_N = B.ROW_N + 1
              And (
                A.DERV_PRTF_ROLE_C <> B.DERV_PRTF_ROLE_C
                Or
                A.EFFT_D <> B.EFFT_D
              )
          ) DT1
          On C.INT_GRUP_I = DT1.INT_GRUP_I
          And C.ROLE_PLAY_I = DT1.ROLE_PLAY_I
          And C.EFFT_D = DT1.EFFT_D
          And C.ROW_N >= DT1.ROW_N
          And DT1.PERD_D <= C.PERD_D 

      ) DT2
  ) DT3
;
```

#### 🔍 Syntax Validation Details:
- **Valid**: ❌

#### 🎯 Teradata Features Detected:
- ADD_MONTHS function
- Variable substitution

### SQL Block 7 (Lines 284-295)
- **Complexity Score**: 62
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
  And A.ROLE_PLAY_I = B.ROLE_PLAY_I
  And A.ROLE_PLAY_TYPE_X = B.ROLE_PLAY_TYPE_X
  And A.DERV_PRTF_ROLE_C = B.DERV_PRTF_ROLE_C
  And (A.JOIN_FROM_D,A.JOIN_TO_D) overlaps (B.JOIN_FROM_D,B.JOIN_TO_D)    
;
```

#### ❄️ Converted Snowflake SQL:
```sql
DELETE A FROM %%STARDATADB%%.DERV_PRTF_OWN_PSST AS A, %%VTECH%%.DERV_PRTF_OWN_HIST_PSST AS B WHERE A.INT_GRUP_I = B.INT_GRUP_I AND A.ROLE_PLAY_I = B.ROLE_PLAY_I AND A.ROLE_PLAY_TYPE_X = B.ROLE_PLAY_TYPE_X AND A.DERV_PRTF_ROLE_C = B.DERV_PRTF_ROLE_C AND (A.JOIN_FROM_D, A.JOIN_TO_D) OVERLAPS (B.JOIN_FROM_D, B.JOIN_TO_D)
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
  AND "a"."role_play_i" = "b"."role_play_i"
  AND "a"."role_play_type_x" = "b"."role_play_type_x"
  AND ("a"."join_from_d", "a"."join_to_d") OVERLAPS ("b"."join_from_d", "b"."join_to_d")
```

#### 📊 SQL Metadata:
- **Tables**: A, DERV_PRTF_OWN_HIST_PSST, DERV_PRTF_OWN_PSST
- **Columns**: DERV_PRTF_ROLE_C, INT_GRUP_I, JOIN_FROM_D, JOIN_TO_D, ROLE_PLAY_I, ROLE_PLAY_TYPE_X
- **Functions**: A.INT_GRUP_I = B.INT_GRUP_I, A.INT_GRUP_I = B.INT_GRUP_I AND A.ROLE_PLAY_I = B.ROLE_PLAY_I, A.INT_GRUP_I = B.INT_GRUP_I AND A.ROLE_PLAY_I = B.ROLE_PLAY_I AND A.ROLE_PLAY_TYPE_X = B.ROLE_PLAY_TYPE_X, A.INT_GRUP_I = B.INT_GRUP_I AND A.ROLE_PLAY_I = B.ROLE_PLAY_I AND A.ROLE_PLAY_TYPE_X = B.ROLE_PLAY_TYPE_X AND A.DERV_PRTF_ROLE_C = B.DERV_PRTF_ROLE_C

### SQL Block 8 (Lines 298-319)
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

*Generated by BTEQ Parser Service - 2025-08-20 12:39:19*
