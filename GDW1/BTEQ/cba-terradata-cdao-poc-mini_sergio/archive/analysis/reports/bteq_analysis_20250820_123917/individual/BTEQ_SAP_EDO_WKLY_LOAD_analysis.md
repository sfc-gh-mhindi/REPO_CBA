# BTEQ_SAP_EDO_WKLY_LOAD.sql - BTEQ Analysis

## File Overview
- **File Name**: BTEQ_SAP_EDO_WKLY_LOAD.sql
- **Analysis Status**: ✅ Success
- **Control Statements**: 32
- **SQL Blocks**: 11

## Control Flow Analysis

| Line | Type | Statement |
|------|------|-----------|
| 1 | RUN | `.RUN FILE=%%BTEQ_LOGON_SCRIPT%%` |
| 2 | IF_ERRORCODE | `.IF ERRORCODE <> 0 THEN .GOTO EXITERR` |
| 75 | IF_ERRORCODE | `.IF ERRORCODE <> 0 THEN .GOTO EXITERR` |
| 100 | IF_ERRORCODE | `.IF ERRORCODE <> 0 THEN .GOTO EXITERR` |
| 119 | IF_ERRORCODE | `.IF ERRORCODE <> 0 THEN .GOTO EXITERR` |
| 141 | IF_ERRORCODE | `.IF ERRORCODE <> 0 THEN .GOTO EXITERR` |
| 161 | IF_ERRORCODE | `.IF ERRORCODE <> 0 THEN .GOTO EXITERR` |
| 183 | IF_ERRORCODE | `.IF ERRORCODE <> 0 THEN .GOTO EXITERR` |
| 234 | IF_ERRORCODE | `.IF ERRORCODE <> 0 THEN .GOTO EXITERR` |
| 316 | IF_ERRORCODE | `.IF ERRORCODE <> 0 THEN .GOTO EXITERR` |
| 331 | IF_ERRORCODE | `.IF ERRORCODE <> 0 THEN .GOTO EXITERR` |
| 335 | IF_ERRORCODE | `.IF ERRORCODE <> 0 THEN .GOTO EXITERR` |
| 405 | IF_ERRORCODE | `	.IF ERRORCODE <> 0 THEN .GOTO EXITERR` |
| 429 | IF_ERRORCODE | `.IF ERRORCODE <> 0 THEN .GOTO EXITERR` |
| 435 | IF_ERRORCODE | `.IF ERRORCODE <> 0 THEN .GOTO EXITERR` |
| 489 | IF_ERRORCODE | `.IF ERRORCODE <> 0 THEN .GOTO EXITERR` |
| 543 | IF_ERRORCODE | `.IF ERRORCODE <> 0 THEN .GOTO EXITERR` |
| 545 | COLLECT_STATS | `COLLECT STATS ON  %%STARDATADB%%.ACCT_REL;` |
| 547 | IF_ERRORCODE | `.IF ERRORCODE <> 0 THEN .GOTO EXITERR` |
| 602 | IF_ERRORCODE | `.IF ERRORCODE <> 0 THEN .GOTO EXITERR` |
| 604 | COLLECT_STATS | `COLLECT STATS ON %%STARDATADB%%.UTIL_PROS_ISAC;` |
| 605 | IF_ERRORCODE | `.IF ERRORCODE <> 0 THEN .GOTO EXITERR` |
| 608 | IF_ERRORCODE | `.IF ERRORCODE <> 0 THEN .GOTO EXITERR` |
| 611 | IF_ERRORCODE | `.IF ERRORCODE <> 0 THEN .GOTO EXITERR` |
| 614 | IF_ERRORCODE | `.IF ERRORCODE <> 0 THEN .GOTO EXITERR` |
| 617 | IF_ERRORCODE | `.IF ERRORCODE <> 0 THEN .GOTO EXITERR` |
| 620 | IF_ERRORCODE | `.IF ERRORCODE <> 0 THEN .GOTO EXITERR` |
| 623 | IF_ERRORCODE | `.IF ERRORCODE <> 0 THEN .GOTO EXITERR` |
| 626 | IF_ERRORCODE | `.IF ERRORCODE <> 0 THEN .GOTO EXITERR` |
| 629 | IF_ERRORCODE | `.IF ERRORCODE <> 0 THEN .GOTO EXITERR` |
| 632 | IF_ERRORCODE | `.IF ERRORCODE <> 0 THEN .GOTO EXITERR` |
| 637 | LABEL | `.LABEL EXITERR` |

## SQL Blocks Analysis

### SQL Block 1 (Lines 72-74)
- **Complexity Score**: 0
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ❌ Invalid
- **Teradata Features**: 0

#### 📝 Original Teradata SQL:
```sql
WITH DATA PRIMARY INDEX(OFFSET_MC_ID)
ON COMMIT PRESERVE ROWS;
```

#### ❄️ Converted Snowflake SQL:
```sql
WITH DATA PRIMARY INDEX(OFFSET_MC_ID)
ON COMMIT PRESERVE ROWS;
```

#### 🔍 Syntax Validation Details:
- **Valid**: ❌

### SQL Block 2 (Lines 187-233)
- **Complexity Score**: 0
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ❌ Invalid
- **Teradata Features**: 2

#### 📝 Original Teradata SQL:
```sql
SELECT 
           MCMDG.INTR_CNCT_NUMB_OF_MAIN_CNCT,
           MCMDG.ACCT_I,
           MCMDG.SRCE_SYST_ISAC_CODE AS MCMDG_SRCE_SYST_ISAC_CODE,
           MCMDG.LOAD_S AS MCMDG_LOAD_S,
           CH.CNCT,
           CH.SRCE_SYST_ISAC_CODE AS CH_SRCE_SYST_ISAC_CODE,
           CH.VALD_FROM,
           CH.LOAD_S AS CH_LOAD_S,
           CH.CTCT_STUS,
           UTIL.RUN_STRM_PROS_D ,
           MCMDG.ACTL_VALD_END ,
           CH.PDCT
           ,UTIL.EXT_FROM_S
           ,UTIL.EXT_TO_S
           ,UTIL.SRCE_SYST_ISAC
      FROM 
           (SEL * FROM  %%VCBODS%%.MSTR_CNCT_MSTR_DATA_GENL WHERE
(INTR_CNCT_NUMB_OF_MAIN_CNCT,SRCE_SYST_ISAC_CODE,LOAD_S ) IN    
(SELECT  INTR_CNCT_NUMB_OF_MAIN_CNCT,SRCE_SYST_ISAC_CODE,
MAX(LOAD_S) FROM  %%VCBODS%%.MSTR_CNCT_MSTR_DATA_GENL GROUP BY 1,2) and Mstr_Cnct_numb in (
Sel OFFSET_MC_NUMBER from  HLS_DIFF_ODS_GDW_1710 WHERE ACCT_STATUS='OPEN' AND EXPIRE_OF_LINKAGE='9999-12-31')) AS MCMDG
            INNER JOIN
           (
           SELECT 
           SRCE_SYST_ISAC
           ,EXT_FROM_S
           ,EXT_TO_S
           ,RUN_STRM_PROS_D
           FROM  %%UTILSTG%%.CBM_UTIL_RUN_STRM_OCCR_CNTL_H 
           WHERE  RUN_STRM_C='SAP00'
           GROUP BY 1,2,3,4
           )UTIL
           ON UTIL.SRCE_SYST_ISAC=MCMDG.SRCE_SYST_ISAC_CODE
           AND MCMDG.LOAD_S <= UTIL.EXT_TO_S 
           AND MCMDG.VALD_STRT <= UTIL.RUN_STRM_PROS_D
           INNER JOIN  %%VCBODS%%.CNCT_HEAD CH
           ON CH.CNCT = MCMDG.INTR_CNCT_NUMB_OF_MAIN_CNCT
           AND CH.SRCE_SYST_ISAC_CODE = MCMDG.SRCE_SYST_ISAC_CODE
           AND CH.LOAD_S <= UTIL.EXT_TO_S
           AND CH.VALD_FROM <= UTIL.RUN_STRM_PROS_D 
           QUALIFY RANK() OVER(PARTITION BY MCMDG.INTR_CNCT_NUMB_OF_MAIN_CNCT,MCMDG.SRCE_SYST_ISAC_CODE
           ORDER BY MCMDG.LOAD_S DESC,MCMDG.VALD_STRT DESC,MCMDG.CHNG_TIME_STMP DESC,CH.LOAD_S DESC,CH.VALD_FROM DESC) = 1
		   )
		   WITH DATA PRIMARY INDEX(INTR_CNCT_NUMB_OF_MAIN_CNCT)
		   ON COMMIT PRESERVE ROWS;
```

#### ❄️ Converted Snowflake SQL:
```sql
SELECT 
           MCMDG.INTR_CNCT_NUMB_OF_MAIN_CNCT,
           MCMDG.ACCT_I,
           MCMDG.SRCE_SYST_ISAC_CODE AS MCMDG_SRCE_SYST_ISAC_CODE,
           MCMDG.LOAD_S AS MCMDG_LOAD_S,
           CH.CNCT,
           CH.SRCE_SYST_ISAC_CODE AS CH_SRCE_SYST_ISAC_CODE,
           CH.VALD_FROM,
           CH.LOAD_S AS CH_LOAD_S,
           CH.CTCT_STUS,
           UTIL.RUN_STRM_PROS_D ,
           MCMDG.ACTL_VALD_END ,
           CH.PDCT
           ,UTIL.EXT_FROM_S
           ,UTIL.EXT_TO_S
           ,UTIL.SRCE_SYST_ISAC
      FROM 
           (SEL * FROM  %%VCBODS%%.MSTR_CNCT_MSTR_DATA_GENL WHERE
(INTR_CNCT_NUMB_OF_MAIN_CNCT,SRCE_SYST_ISAC_CODE,LOAD_S ) IN    
(SELECT  INTR_CNCT_NUMB_OF_MAIN_CNCT,SRCE_SYST_ISAC_CODE,
MAX(LOAD_S) FROM  %%VCBODS%%.MSTR_CNCT_MSTR_DATA_GENL GROUP BY 1,2) and Mstr_Cnct_numb in (
Sel OFFSET_MC_NUMBER from  HLS_DIFF_ODS_GDW_1710 WHERE ACCT_STATUS='OPEN' AND EXPIRE_OF_LINKAGE='9999-12-31')) AS MCMDG
            INNER JOIN
           (
           SELECT 
           SRCE_SYST_ISAC
           ,EXT_FROM_S
           ,EXT_TO_S
           ,RUN_STRM_PROS_D
           FROM  %%UTILSTG%%.CBM_UTIL_RUN_STRM_OCCR_CNTL_H 
           WHERE  RUN_STRM_C='SAP00'
           GROUP BY 1,2,3,4
           )UTIL
           ON UTIL.SRCE_SYST_ISAC=MCMDG.SRCE_SYST_ISAC_CODE
           AND MCMDG.LOAD_S <= UTIL.EXT_TO_S 
           AND MCMDG.VALD_STRT <= UTIL.RUN_STRM_PROS_D
           INNER JOIN  %%VCBODS%%.CNCT_HEAD CH
           ON CH.CNCT = MCMDG.INTR_CNCT_NUMB_OF_MAIN_CNCT
           AND CH.SRCE_SYST_ISAC_CODE = MCMDG.SRCE_SYST_ISAC_CODE
           AND CH.LOAD_S <= UTIL.EXT_TO_S
           AND CH.VALD_FROM <= UTIL.RUN_STRM_PROS_D 
           QUALIFY RANK() OVER(PARTITION BY MCMDG.INTR_CNCT_NUMB_OF_MAIN_CNCT,MCMDG.SRCE_SYST_ISAC_CODE
           ORDER BY MCMDG.LOAD_S DESC,MCMDG.VALD_STRT DESC,MCMDG.CHNG_TIME_STMP DESC,CH.LOAD_S DESC,CH.VALD_FROM DESC) = 1
		   )
		   WITH DATA PRIMARY INDEX(INTR_CNCT_NUMB_OF_MAIN_CNCT)
		   ON COMMIT PRESERVE ROWS;
```

#### 🔍 Syntax Validation Details:
- **Valid**: ❌

#### 🎯 Teradata Features Detected:
- QUALIFY clause
- Variable substitution

### SQL Block 3 (Lines 239-315)
- **Complexity Score**: 0
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ❌ Invalid
- **Teradata Features**: 2

#### 📝 Original Teradata SQL:
```sql
SELECT	
MCL.ACCT_I AS SUBJ_ACCT_I,
CASE WHEN MCBTP.CNCT_CATG = '0001' 
and	MCPA.NON_SAP_ACCT_IDNN  is NULL THEN AMD.ACCT_I   
WHEN MCBTP.CNCT_CATG = '0001' and	MCPA.NON_SAP_ACCT_IDNN  is NOT NULL THEN WNSA.ACCT_I  WHEN MCBTP.CNCT_CATG = '0003' THEN MCL.ACCT_I ELSE NULL 
END	AS OBJC_ACCT_I ,
MCBTP.OBJC_BDT_APPT,
MCBTP.VALD_FROM,
MCBTP.ACTL_VALD_END,
CASE WHEN MSAR.REL_C IS NULL THEN '9999' ELSE	MSAR.REL_C END	AS REL_C,
CASE WHEN MSAR.MIRR_REL_C IS NULL THEN '9999' ELSE	MSAR.MIRR_REL_C END	AS MIRR_REL_C,
MCL.CTCT_STUS,
MCBTP.INTR_CNCT_NUMB_OF_MAIN_CNCT,
MCBTP.HIER_ID,
MCBTP.CNCT_CATG,
CASE	WHEN MCBTP.INTR_CNCT_NUMB_OF_MAIN_CNCT=MCBTP.HIER_ID THEN 0 Else	1 END	AS LAVEL,
MCL.RUN_STRM_PROS_D FROM
--(SEL * FROM  %%VCBODS%%.MSTR_CNCT_BALN_TRNF_PRTP WHERE INTR_CNCT_NUMB_OF_MAIN_CNCT='EAE1C3831B5B23C0E10080000A1F20D4' ) AS MCBTP
 %%VCBODS%%.MSTR_CNCT_BALN_TRNF_PRTP   as MCBTP
INNER JOIN        /*CONTROL TABLE ENTRY HAS BEEN INCLUDED  FOR THE PURPOSE OF PROPBLEM TICKET#PM0024182*/
(--SELECT MAX(EXT_FROM_S) AS EXT_FROM_S,MAX(EXT_TO_S) AS EXT_TO_S
SELECT EXT_FROM_S,EXT_TO_S FROM  %%UTILSTG%%.CBM_UTIL_RUN_STRM_OCCR_CNTL_H WHERE
RUN_STRM_C='SAP00' --AND RUN_STRM_PROS_D='2018-08-24'
) AS CCEDW
ON MCBTP.LOAD_S<=CCEDW.EXT_TO_S
INNER JOIN  MCMDG_CH_L3BR21 MCL
ON MCBTP.INTR_CNCT_NUMB_OF_MAIN_CNCT=MCL.INTR_CNCT_NUMB_OF_MAIN_CNCT
AND MCBTP.SRCE_SYST_ISAC_CODE=MCL.MCMDG_SRCE_SYST_ISAC_CODE
AND (MCBTP.CNCT_CATG = '0001' OR MCBTP.CNCT_CATG = '0003') 
AND (MCBTP.LOAD_S <=MCL.EXT_TO_S AND COALESCE(MCBTP.UPD_LOAD_S,TIMESTAMP '9999-12-31 00:00:00.000000')>MCL.EXT_TO_S)
AND MCBTP.VALD_FROM<=MCL.RUN_STRM_PROS_D
LEFT JOIN  %%VCBODS%%.MSTR_CNCT_PRXY_ACCT MCPA
ON MCPA.ELEM_OF_A_CNCT_HIER_GRUP=MCBTP.ELEM_OF_A_CNCT_HIER_GRUP
AND MCPA.SRCE_SYST_ISAC_CODE=MCBTP.SRCE_SYST_ISAC_CODE
AND ( MCPA.CNCT_TYPE_KEY=5 OR MCPA.CNCT_TYPE_KEY=6)
AND MCPA.LOAD_S<=MCL.EXT_TO_S
LEFT JOIN  %%VCBODS%%.ACCT_MSTR_DATA AMD
ON AMD.ACCT = MCPA.EXTL_CNCT_PART_1
AND AMD.SRCE_SYST_ISAC_CODE = MCPA.SRCE_SYST_ISAC_CODE 
AND (AMD.LOAD_S <=MCL.EXT_TO_S AND COALESCE(AMD.UPD_LOAD_S,TIMESTAMP '9999-12-31 00:00:00.000000')> MCL.EXT_TO_S)
LEFT JOIN 
 %%VCBODS%%.WUL_NON_SAP_ACCT WNSA
ON MCPA.NON_SAP_ACCT_NUMB = WNSA.SRCE_SYST_ACCT_NUMB
--AND CAST(MCPA.CIF_PDCT_IDNN AS DECIMAL(4,0)) = WNSA.PDCT_IDNN 
AND MCPA.APPT_CODE =WNSA.NON_SAP_CNCT_APPT_CODE 
--AND WNSA.LOAD_S<=MCL.EXT_TO_S
LEFT JOIN %%VTECH%%.MAP_SAP_INVL_PDCT MSIP
ON AMD.PDCT =MSIP.PDCT
AND CAST( MCL.RUN_STRM_PROS_D  AS DATE FORMAT 'YYYYMMDD') BETWEEN MSIP.EFFT_D AND MSIP.EXPY_D 
LEFT OUTER JOIN %%VTECH%%.MAP_SAP_ACCT_REL MSAR
ON MSAR.SRCE_TABL_M='MSTR_CNCT_BALN_TRNF_PRTP' 
AND MSAR.SRCE_COLM_M='OBJC_BDT_APPT'
AND MSAR.SRCE_VALU_C=MCBTP.OBJC_BDT_APPT
AND CAST( MCL.RUN_STRM_PROS_D  AS DATE FORMAT 'YYYYMMDD') BETWEEN MSAR.EFFT_D AND MSAR.EXPY_D 
WHERE
(
--MCBTP.LOAD_S >=MCL.EXT_FROM_S  
MCBTP.LOAD_S >CCEDW.EXT_FROM_S
OR 
MCL.CH_LOAD_S>=MCL.EXT_FROM_S
OR 
MCL.MCMDG_LOAD_S>=MCL.EXT_FROM_S
OR 
MCPA.LOAD_S >=MCL.EXT_FROM_S
)
AND
OBJC_ACCT_I IS NOT NULL
AND SUBJ_ACCT_I<>OBJC_ACCT_I
AND COALESCE (MSIP.PDCT_C,'') <> 'INVL'
QUALIFY
RANK()OVER(PARTITION BY MCBTP.HIER_ID, MCBTP.SRCE_SYST_ISAC_CODE ORDER BY MCBTP.LOAD_S DESC)=1
AND RANK()OVER(PARTITION BY MCPA.ELEM_OF_A_CNCT_HIER_GRUP, MCPA.SRCE_SYST_ISAC_CODE ORDER BY MCPA.LOAD_S DESC)=1
AND RANK()OVER(PARTITION BY WNSA.SRCE_SYST_ACCT_NUMB, WNSA.PDCT_IDNN, WNSA.NON_SAP_CNCT_APPT_CODE ORDER BY WNSA.LOAD_S DESC)=1
		   )
		   WITH DATA PRIMARY INDEX(SUBJ_ACCT_I)
		   ON COMMIT PRESERVE ROWS;
```

#### ❄️ Converted Snowflake SQL:
```sql
SELECT	
MCL.ACCT_I AS SUBJ_ACCT_I,
CASE WHEN MCBTP.CNCT_CATG = '0001' 
and	MCPA.NON_SAP_ACCT_IDNN  is NULL THEN AMD.ACCT_I   
WHEN MCBTP.CNCT_CATG = '0001' and	MCPA.NON_SAP_ACCT_IDNN  is NOT NULL THEN WNSA.ACCT_I  WHEN MCBTP.CNCT_CATG = '0003' THEN MCL.ACCT_I ELSE NULL 
END	AS OBJC_ACCT_I ,
MCBTP.OBJC_BDT_APPT,
MCBTP.VALD_FROM,
MCBTP.ACTL_VALD_END,
CASE WHEN MSAR.REL_C IS NULL THEN '9999' ELSE	MSAR.REL_C END	AS REL_C,
CASE WHEN MSAR.MIRR_REL_C IS NULL THEN '9999' ELSE	MSAR.MIRR_REL_C END	AS MIRR_REL_C,
MCL.CTCT_STUS,
MCBTP.INTR_CNCT_NUMB_OF_MAIN_CNCT,
MCBTP.HIER_ID,
MCBTP.CNCT_CATG,
CASE	WHEN MCBTP.INTR_CNCT_NUMB_OF_MAIN_CNCT=MCBTP.HIER_ID THEN 0 Else	1 END	AS LAVEL,
MCL.RUN_STRM_PROS_D FROM
--(SEL * FROM  %%VCBODS%%.MSTR_CNCT_BALN_TRNF_PRTP WHERE INTR_CNCT_NUMB_OF_MAIN_CNCT='EAE1C3831B5B23C0E10080000A1F20D4' ) AS MCBTP
 %%VCBODS%%.MSTR_CNCT_BALN_TRNF_PRTP   as MCBTP
INNER JOIN        /*CONTROL TABLE ENTRY HAS BEEN INCLUDED  FOR THE PURPOSE OF PROPBLEM TICKET#PM0024182*/
(--SELECT MAX(EXT_FROM_S) AS EXT_FROM_S,MAX(EXT_TO_S) AS EXT_TO_S
SELECT EXT_FROM_S,EXT_TO_S FROM  %%UTILSTG%%.CBM_UTIL_RUN_STRM_OCCR_CNTL_H WHERE
RUN_STRM_C='SAP00' --AND RUN_STRM_PROS_D='2018-08-24'
) AS CCEDW
ON MCBTP.LOAD_S<=CCEDW.EXT_TO_S
INNER JOIN  MCMDG_CH_L3BR21 MCL
ON MCBTP.INTR_CNCT_NUMB_OF_MAIN_CNCT=MCL.INTR_CNCT_NUMB_OF_MAIN_CNCT
AND MCBTP.SRCE_SYST_ISAC_CODE=MCL.MCMDG_SRCE_SYST_ISAC_CODE
AND (MCBTP.CNCT_CATG = '0001' OR MCBTP.CNCT_CATG = '0003') 
AND (MCBTP.LOAD_S <=MCL.EXT_TO_S AND COALESCE(MCBTP.UPD_LOAD_S,TIMESTAMP '9999-12-31 00:00:00.000000')>MCL.EXT_TO_S)
AND MCBTP.VALD_FROM<=MCL.RUN_STRM_PROS_D
LEFT JOIN  %%VCBODS%%.MSTR_CNCT_PRXY_ACCT MCPA
ON MCPA.ELEM_OF_A_CNCT_HIER_GRUP=MCBTP.ELEM_OF_A_CNCT_HIER_GRUP
AND MCPA.SRCE_SYST_ISAC_CODE=MCBTP.SRCE_SYST_ISAC_CODE
AND ( MCPA.CNCT_TYPE_KEY=5 OR MCPA.CNCT_TYPE_KEY=6)
AND MCPA.LOAD_S<=MCL.EXT_TO_S
LEFT JOIN  %%VCBODS%%.ACCT_MSTR_DATA AMD
ON AMD.ACCT = MCPA.EXTL_CNCT_PART_1
AND AMD.SRCE_SYST_ISAC_CODE = MCPA.SRCE_SYST_ISAC_CODE 
AND (AMD.LOAD_S <=MCL.EXT_TO_S AND COALESCE(AMD.UPD_LOAD_S,TIMESTAMP '9999-12-31 00:00:00.000000')> MCL.EXT_TO_S)
LEFT JOIN 
 %%VCBODS%%.WUL_NON_SAP_ACCT WNSA
ON MCPA.NON_SAP_ACCT_NUMB = WNSA.SRCE_SYST_ACCT_NUMB
--AND CAST(MCPA.CIF_PDCT_IDNN AS DECIMAL(4,0)) = WNSA.PDCT_IDNN 
AND MCPA.APPT_CODE =WNSA.NON_SAP_CNCT_APPT_CODE 
--AND WNSA.LOAD_S<=MCL.EXT_TO_S
LEFT JOIN %%VTECH%%.MAP_SAP_INVL_PDCT MSIP
ON AMD.PDCT =MSIP.PDCT
AND CAST( MCL.RUN_STRM_PROS_D  AS DATE FORMAT 'YYYYMMDD') BETWEEN MSIP.EFFT_D AND MSIP.EXPY_D 
LEFT OUTER JOIN %%VTECH%%.MAP_SAP_ACCT_REL MSAR
ON MSAR.SRCE_TABL_M='MSTR_CNCT_BALN_TRNF_PRTP' 
AND MSAR.SRCE_COLM_M='OBJC_BDT_APPT'
AND MSAR.SRCE_VALU_C=MCBTP.OBJC_BDT_APPT
AND CAST( MCL.RUN_STRM_PROS_D  AS DATE FORMAT 'YYYYMMDD') BETWEEN MSAR.EFFT_D AND MSAR.EXPY_D 
WHERE
(
--MCBTP.LOAD_S >=MCL.EXT_FROM_S  
MCBTP.LOAD_S >CCEDW.EXT_FROM_S
OR 
MCL.CH_LOAD_S>=MCL.EXT_FROM_S
OR 
MCL.MCMDG_LOAD_S>=MCL.EXT_FROM_S
OR 
MCPA.LOAD_S >=MCL.EXT_FROM_S
)
AND
OBJC_ACCT_I IS NOT NULL
AND SUBJ_ACCT_I<>OBJC_ACCT_I
AND COALESCE (MSIP.PDCT_C,'') <> 'INVL'
QUALIFY
RANK()OVER(PARTITION BY MCBTP.HIER_ID, MCBTP.SRCE_SYST_ISAC_CODE ORDER BY MCBTP.LOAD_S DESC)=1
AND RANK()OVER(PARTITION BY MCPA.ELEM_OF_A_CNCT_HIER_GRUP, MCPA.SRCE_SYST_ISAC_CODE ORDER BY MCPA.LOAD_S DESC)=1
AND RANK()OVER(PARTITION BY WNSA.SRCE_SYST_ACCT_NUMB, WNSA.PDCT_IDNN, WNSA.NON_SAP_CNCT_APPT_CODE ORDER BY WNSA.LOAD_S DESC)=1
		   )
		   WITH DATA PRIMARY INDEX(SUBJ_ACCT_I)
		   ON COMMIT PRESERVE ROWS;
```

#### 🔍 Syntax Validation Details:
- **Valid**: ❌

#### 🎯 Teradata Features Detected:
- QUALIFY clause
- Variable substitution

### SQL Block 4 (Lines 328-330)
- **Complexity Score**: 0
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ❌ Invalid
- **Teradata Features**: 0

#### 📝 Original Teradata SQL:
```sql
WITH DATA PRIMARY INDEX(SUBJ_ACCT_I)
		   ON COMMIT PRESERVE ROWS;
```

#### ❄️ Converted Snowflake SQL:
```sql
WITH DATA PRIMARY INDEX(SUBJ_ACCT_I)
		   ON COMMIT PRESERVE ROWS;
```

#### 🔍 Syntax Validation Details:
- **Valid**: ❌

### SQL Block 5 (Lines 333-334)
- **Complexity Score**: 4
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
DELETE FROM  %%DDSTG%%.ACCT_REL_HLS_REME;
```

#### ❄️ Converted Snowflake SQL:
```sql
DELETE FROM %%DDSTG%%.ACCT_REL_HLS_REME
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
DELETE FROM "%%ddstg%%"."acct_rel_hls_reme"
```

#### 📊 SQL Metadata:
- **Tables**: ACCT_REL_HLS_REME

### SQL Block 6 (Lines 337-404)
- **Complexity Score**: 309
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
INSERT INTO  %%DDSTG%%.ACCT_REL_HLS_REME 
 SELECT
	  SRC.SUBJ_ACCT_I
	  ,SRC.OBJC_ACCT_I
	  ,SRC.OBJC_BDT_APPT
	  ,SRC.VALD_FROM
	  ,SRC.ACTL_VALD_END
	  ,SRC.REL_C
	  ,SRC.MIRR_REL_C
	  ,SRC.CTCT_STUS
	  ,SRC.INTR_CNCT_NUMB_OF_MAIN_CNCT
	  ,SRC.HIER_ID
	  ,SRC.CNCT_CATG
	  ,SRC.RUN_STRM_PROS_D
	  ,SRC.OBJC_ACCT_LEVL_N
	  ,SRC.SUBJ_ACCT_LEVL_N
	  ,AR.EFFT_D AS TGT_EFFT_D
	  ,CASE WHEN  AR.SUBJ_ACCT_I IS NULL AND SRC.ACTL_VALD_END > SRC.RUN_STRM_PROS_D THEN 'I'
	       WHEN  AR.SUBJ_ACCT_I=SRC.SUBJ_ACCT_I  AND ( SRC.SUBJ_ACCT_LEVL_N <> AR.SUBJ_ACCT_LEVL_N OR SRC.OBJC_ACCT_LEVL_N <> AR.OBJC_ACCT_LEVL_N) AND SRC.ACTL_VALD_END > SRC.RUN_STRM_PROS_D THEN 'U'              
	       WHEN  AR.SUBJ_ACCT_I=SRC.SUBJ_ACCT_I  AND ( SRC.CTCT_STUS='50' OR SRC.ACTL_VALD_END <= SRC.RUN_STRM_PROS_D) THEN 'D'
	       ELSE 'C'
	  END AS FLG
	  FROM
	  (
	  SELECT
	  ARTP.SUBJ_ACCT_I
	     ,ARTP.OBJC_ACCT_I
	     ,ARTP.OBJC_BDT_APPT
	     ,ARTP.VALD_FROM
	     ,ARTP.ACTL_VALD_END
	     ,ARTP.REL_C
	     ,ARTP.MIRR_REL_C
	     ,ARTP.CTCT_STUS
	     ,ARTP.INTR_CNCT_NUMB_OF_MAIN_CNCT
	     ,ARTP.HIER_ID                       
	     ,ARTP.CNCT_CATG                     
	     ,ARTP.LAVEL                         
	     ,ARTP.RUN_STRM_PROS_D
	     ,ARTP1.OBJC_ACCT_LEVL_N
	     ,CASE WHEN ARTP2.SUBJ_ACCT_LEVL_N IS NULL THEN 1 ELSE ARTP2.SUBJ_ACCT_LEVL_N END AS SUBJ_ACCT_LEVL_N
	     FROM
	      ACCT_REL_TRNF_PRTP_2 ARTP
	     LEFT OUTER JOIN
	     (SELECT COUNT(*)+1 AS OBJC_ACCT_LEVL_N,OBJC_ACCT_I,HIER_ID
	      FROM  ACCT_REL_TRNF_PRTP_2
	      GROUP BY OBJC_ACCT_I,HIER_ID
	     )ARTP1
	     ON ARTP.OBJC_ACCT_I=ARTP1.OBJC_ACCT_I
	     AND ARTP.HIER_ID=ARTP1.HIER_ID
	     LEFT OUTER JOIN
	     (
	      SELECT COUNT(*)+1 AS SUBJ_ACCT_LEVL_N,OBJC_ACCT_I,HIER_ID
	      FROM  ACCT_REL_TRNF_PRTP_2
	      GROUP BY OBJC_ACCT_I,HIER_ID
	     )ARTP2
	     ON ARTP.SUBJ_ACCT_I=ARTP2.OBJC_ACCT_I
	     AND ARTP.HIER_ID=ARTP2.HIER_ID       
	  ) SRC
	  LEFT JOIN 
	  
	   %%VTECH%%.ACCT_REL AR
	  ON AR.SUBJ_ACCT_I=SRC.SUBJ_ACCT_I
	  AND AR.OBJC_ACCT_I=SRC.OBJC_ACCT_I
	  AND AR.REL_C=SRC.REL_C
	  AND EXPY_D='9999-12-31'
	  WHERE
	  FLG <>'C' ;
```

#### ❄️ Converted Snowflake SQL:
```sql
INSERT INTO %%DDSTG%%.ACCT_REL_HLS_REME SELECT SRC.SUBJ_ACCT_I, SRC.OBJC_ACCT_I, SRC.OBJC_BDT_APPT, SRC.VALD_FROM, SRC.ACTL_VALD_END, SRC.REL_C, SRC.MIRR_REL_C, SRC.CTCT_STUS, SRC.INTR_CNCT_NUMB_OF_MAIN_CNCT, SRC.HIER_ID, SRC.CNCT_CATG, SRC.RUN_STRM_PROS_D, SRC.OBJC_ACCT_LEVL_N, SRC.SUBJ_ACCT_LEVL_N, AR.EFFT_D AS TGT_EFFT_D, CASE WHEN AR.SUBJ_ACCT_I IS NULL AND SRC.ACTL_VALD_END > SRC.RUN_STRM_PROS_D THEN 'I' WHEN AR.SUBJ_ACCT_I = SRC.SUBJ_ACCT_I AND (SRC.SUBJ_ACCT_LEVL_N <> AR.SUBJ_ACCT_LEVL_N OR SRC.OBJC_ACCT_LEVL_N <> AR.OBJC_ACCT_LEVL_N) AND SRC.ACTL_VALD_END > SRC.RUN_STRM_PROS_D THEN 'U' WHEN AR.SUBJ_ACCT_I = SRC.SUBJ_ACCT_I AND (SRC.CTCT_STUS = '50' OR SRC.ACTL_VALD_END <= SRC.RUN_STRM_PROS_D) THEN 'D' ELSE 'C' END AS FLG FROM (SELECT ARTP.SUBJ_ACCT_I, ARTP.OBJC_ACCT_I, ARTP.OBJC_BDT_APPT, ARTP.VALD_FROM, ARTP.ACTL_VALD_END, ARTP.REL_C, ARTP.MIRR_REL_C, ARTP.CTCT_STUS, ARTP.INTR_CNCT_NUMB_OF_MAIN_CNCT, ARTP.HIER_ID, ARTP.CNCT_CATG, ARTP.LAVEL, ARTP.RUN_STRM_PROS_D, ARTP1.OBJC_ACCT_LEVL_N, CASE WHEN ARTP2.SUBJ_ACCT_LEVL_N IS NULL THEN 1 ELSE ARTP2.SUBJ_ACCT_LEVL_N END AS SUBJ_ACCT_LEVL_N FROM ACCT_REL_TRNF_PRTP_2 AS ARTP LEFT OUTER JOIN (SELECT COUNT(*) + 1 AS OBJC_ACCT_LEVL_N, OBJC_ACCT_I, HIER_ID FROM ACCT_REL_TRNF_PRTP_2 GROUP BY OBJC_ACCT_I, HIER_ID) AS ARTP1 ON ARTP.OBJC_ACCT_I = ARTP1.OBJC_ACCT_I AND ARTP.HIER_ID = ARTP1.HIER_ID LEFT OUTER JOIN (SELECT COUNT(*) + 1 AS SUBJ_ACCT_LEVL_N, OBJC_ACCT_I, HIER_ID FROM ACCT_REL_TRNF_PRTP_2 GROUP BY OBJC_ACCT_I, HIER_ID) AS ARTP2 ON ARTP.SUBJ_ACCT_I = ARTP2.OBJC_ACCT_I AND ARTP.HIER_ID = ARTP2.HIER_ID) AS SRC LEFT JOIN %%VTECH%%.ACCT_REL AS AR ON AR.SUBJ_ACCT_I = SRC.SUBJ_ACCT_I AND AR.OBJC_ACCT_I = SRC.OBJC_ACCT_I AND AR.REL_C = SRC.REL_C AND EXPY_D = '9999-12-31' WHERE FLG <> 'C'
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
INSERT INTO "%%ddstg%%"."acct_rel_hls_reme"
WITH "artp1" AS (
  SELECT
    COUNT(*) + 1 AS "objc_acct_levl_n",
    "acct_rel_trnf_prtp_2"."objc_acct_i" AS "objc_acct_i",
    "acct_rel_trnf_prtp_2"."hier_id" AS "hier_id"
  FROM "acct_rel_trnf_prtp_2" AS "acct_rel_trnf_prtp_2"
  GROUP BY
    "acct_rel_trnf_prtp_2"."objc_acct_i",
    "acct_rel_trnf_prtp_2"."hier_id"
), "artp2" AS (
  SELECT
    COUNT(*) + 1 AS "subj_acct_levl_n",
    "acct_rel_trnf_prtp_2"."objc_acct_i" AS "objc_acct_i",
    "acct_rel_trnf_prtp_2"."hier_id" AS "hier_id"
  FROM "acct_rel_trnf_prtp_2" AS "acct_rel_trnf_prtp_2"
  GROUP BY
    "acct_rel_trnf_prtp_2"."objc_acct_i",
    "acct_rel_trnf_prtp_2"."hier_id"
)
SELECT
  "artp"."subj_acct_i" AS "subj_acct_i",
  "artp"."objc_acct_i" AS "objc_acct_i",
  "artp"."objc_bdt_appt" AS "objc_bdt_appt",
  "artp"."vald_from" AS "vald_from",
  "artp"."actl_vald_end" AS "actl_vald_end",
  "artp"."rel_c" AS "rel_c",
  "artp"."mirr_rel_c" AS "mirr_rel_c",
  "artp"."ctct_stus" AS "ctct_stus",
  "artp"."intr_cnct_numb_of_main_cnct" AS "intr_cnct_numb_of_main_cnct",
  "artp"."hier_id" AS "hier_id",
  "artp"."cnct_catg" AS "cnct_catg",
  "artp"."run_strm_pros_d" AS "run_strm_pros_d",
  "artp1"."objc_acct_levl_n" AS "objc_acct_levl_n",
  CASE
    WHEN "artp2"."subj_acct_levl_n" IS NULL
    THEN 1
    ELSE "artp2"."subj_acct_levl_n"
  END AS "subj_acct_levl_n",
  "ar"."efft_d" AS "tgt_efft_d",
  CASE
    WHEN "ar"."subj_acct_i" IS NULL AND "artp"."actl_vald_end" > "artp"."run_strm_pros_d"
    THEN 'I'
    WHEN (
      "ar"."objc_acct_levl_n" <> "artp1"."objc_acct_levl_n"
      OR "ar"."subj_acct_levl_n" <> CASE
        WHEN "artp2"."subj_acct_levl_n" IS NULL
        THEN 1
        ELSE "artp2"."subj_acct_levl_n"
      END
    )
    AND "ar"."subj_acct_i" = "artp"."subj_acct_i"
    AND "artp"."actl_vald_end" > "artp"."run_strm_pros_d"
    THEN 'U'
    WHEN "ar"."subj_acct_i" = "artp"."subj_acct_i"
    AND (
      "artp"."actl_vald_end" <= "artp"."run_strm_pros_d" OR "artp"."ctct_stus" = '50'
    )
    THEN 'D'
    ELSE 'C'
  END AS "flg"
FROM "acct_rel_trnf_prtp_2" AS "artp"
LEFT JOIN "artp1" AS "artp1"
  ON "artp"."hier_id" = "artp1"."hier_id"
  AND "artp"."objc_acct_i" = "artp1"."objc_acct_i"
LEFT JOIN "artp2" AS "artp2"
  ON "artp"."hier_id" = "artp2"."hier_id"
  AND "artp"."subj_acct_i" = "artp2"."objc_acct_i"
LEFT JOIN "%%vtech%%"."acct_rel" AS "ar"
  ON "ar"."expy_d" = '9999-12-31'
  AND "ar"."objc_acct_i" = "artp"."objc_acct_i"
  AND "ar"."rel_c" = "artp"."rel_c"
  AND "ar"."subj_acct_i" = "artp"."subj_acct_i"
WHERE
  CASE
    WHEN "ar"."subj_acct_i" IS NULL AND "artp"."actl_vald_end" > "artp"."run_strm_pros_d"
    THEN 'I'
    WHEN (
      "ar"."objc_acct_levl_n" <> "artp1"."objc_acct_levl_n"
      OR "ar"."subj_acct_levl_n" <> CASE
        WHEN "artp2"."subj_acct_levl_n" IS NULL
        THEN 1
        ELSE "artp2"."subj_acct_levl_n"
      END
    )
    AND "ar"."subj_acct_i" = "artp"."subj_acct_i"
    AND "artp"."actl_vald_end" > "artp"."run_strm_pros_d"
    THEN 'U'
    WHEN "ar"."subj_acct_i" = "artp"."subj_acct_i"
    AND (
      "artp"."actl_vald_end" <= "artp"."run_strm_pros_d" OR "artp"."ctct_stus" = '50'
    )
    THEN 'D'
    ELSE 'C'
  END <> 'C'
```

#### 📊 SQL Metadata:
- **Tables**: ACCT_REL, ACCT_REL_HLS_REME, ACCT_REL_TRNF_PRTP_2
- **Columns**: ACTL_VALD_END, CNCT_CATG, CTCT_STUS, EFFT_D, EXPY_D, FLG, HIER_ID, INTR_CNCT_NUMB_OF_MAIN_CNCT, LAVEL, MIRR_REL_C, OBJC_ACCT_I, OBJC_ACCT_LEVL_N, OBJC_BDT_APPT, REL_C, RUN_STRM_PROS_D, SUBJ_ACCT_I, SUBJ_ACCT_LEVL_N, VALD_FROM
- **Functions**: *, AR.SUBJ_ACCT_I = SRC.SUBJ_ACCT_I, AR.SUBJ_ACCT_I = SRC.SUBJ_ACCT_I AND (SRC.CTCT_STUS = '50' OR SRC.ACTL_VALD_END <= SRC.RUN_STRM_PROS_D), AR.SUBJ_ACCT_I = SRC.SUBJ_ACCT_I AND (SRC.SUBJ_ACCT_LEVL_N <> AR.SUBJ_ACCT_LEVL_N OR SRC.OBJC_ACCT_LEVL_N <> AR.OBJC_ACCT_LEVL_N), AR.SUBJ_ACCT_I = SRC.SUBJ_ACCT_I AND (SRC.SUBJ_ACCT_LEVL_N <> AR.SUBJ_ACCT_LEVL_N OR SRC.OBJC_ACCT_LEVL_N <> AR.OBJC_ACCT_LEVL_N) AND SRC.ACTL_VALD_END > SRC.RUN_STRM_PROS_D, AR.SUBJ_ACCT_I = SRC.SUBJ_ACCT_I AND AR.OBJC_ACCT_I = SRC.OBJC_ACCT_I, AR.SUBJ_ACCT_I = SRC.SUBJ_ACCT_I AND AR.OBJC_ACCT_I = SRC.OBJC_ACCT_I AND AR.REL_C = SRC.REL_C, AR.SUBJ_ACCT_I IS NULL, AR.SUBJ_ACCT_I IS NULL AND SRC.ACTL_VALD_END > SRC.RUN_STRM_PROS_D, ARTP.OBJC_ACCT_I = ARTP1.OBJC_ACCT_I, ARTP.SUBJ_ACCT_I = ARTP2.OBJC_ACCT_I, ARTP2.SUBJ_ACCT_LEVL_N IS NULL, None, SRC.CTCT_STUS = '50', SRC.SUBJ_ACCT_LEVL_N <> AR.SUBJ_ACCT_LEVL_N

### SQL Block 7 (Lines 407-428)
- **Complexity Score**: 44
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
INSERT INTO  %%DDSTG%%.ACCT_REL_HLS_REME_HIST
SEL
 SUBJ_ACCT_I
,OBJC_ACCT_I
,OBJC_BDT_APPT
,VALD_FROM
,ACTL_VALD_END
,REL_C
,MIRR_REL_C
,CTCT_STUS
,INTR_CNCT_NUMB_OF_MAIN_CNCT
,HIER_ID
,CNCT_CATG
,RUN_STRM_PROS_D
,OBJC_ACCT_LEVL_N
,SUBJ_ACCT_LEVL_N
,TGT_EFFT_D
,FLG
,CURRENT_DATE AS LOAD_DATE
FROM
 %%DDSTG%%.ACCT_REL_HLS_REME;
```

#### ❄️ Converted Snowflake SQL:
```sql
INSERT INTO %%DDSTG%%.ACCT_REL_HLS_REME_HIST SELECT SUBJ_ACCT_I, OBJC_ACCT_I, OBJC_BDT_APPT, VALD_FROM, ACTL_VALD_END, REL_C, MIRR_REL_C, CTCT_STUS, INTR_CNCT_NUMB_OF_MAIN_CNCT, HIER_ID, CNCT_CATG, RUN_STRM_PROS_D, OBJC_ACCT_LEVL_N, SUBJ_ACCT_LEVL_N, TGT_EFFT_D, FLG, CURRENT_DATE AS LOAD_DATE FROM %%DDSTG%%.ACCT_REL_HLS_REME
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
INSERT INTO "%%ddstg%%"."acct_rel_hls_reme_hist"
SELECT
  "acct_rel_hls_reme"."subj_acct_i" AS "subj_acct_i",
  "acct_rel_hls_reme"."objc_acct_i" AS "objc_acct_i",
  "acct_rel_hls_reme"."objc_bdt_appt" AS "objc_bdt_appt",
  "acct_rel_hls_reme"."vald_from" AS "vald_from",
  "acct_rel_hls_reme"."actl_vald_end" AS "actl_vald_end",
  "acct_rel_hls_reme"."rel_c" AS "rel_c",
  "acct_rel_hls_reme"."mirr_rel_c" AS "mirr_rel_c",
  "acct_rel_hls_reme"."ctct_stus" AS "ctct_stus",
  "acct_rel_hls_reme"."intr_cnct_numb_of_main_cnct" AS "intr_cnct_numb_of_main_cnct",
  "acct_rel_hls_reme"."hier_id" AS "hier_id",
  "acct_rel_hls_reme"."cnct_catg" AS "cnct_catg",
  "acct_rel_hls_reme"."run_strm_pros_d" AS "run_strm_pros_d",
  "acct_rel_hls_reme"."objc_acct_levl_n" AS "objc_acct_levl_n",
  "acct_rel_hls_reme"."subj_acct_levl_n" AS "subj_acct_levl_n",
  "acct_rel_hls_reme"."tgt_efft_d" AS "tgt_efft_d",
  "acct_rel_hls_reme"."flg" AS "flg",
  CURRENT_DATE AS "load_date"
FROM "%%ddstg%%"."acct_rel_hls_reme" AS "acct_rel_hls_reme"
```

#### 📊 SQL Metadata:
- **Tables**: ACCT_REL_HLS_REME, ACCT_REL_HLS_REME_HIST
- **Columns**: ACTL_VALD_END, CNCT_CATG, CTCT_STUS, FLG, HIER_ID, INTR_CNCT_NUMB_OF_MAIN_CNCT, MIRR_REL_C, OBJC_ACCT_I, OBJC_ACCT_LEVL_N, OBJC_BDT_APPT, REL_C, RUN_STRM_PROS_D, SUBJ_ACCT_I, SUBJ_ACCT_LEVL_N, TGT_EFFT_D, VALD_FROM
- **Functions**: None

### SQL Block 8 (Lines 431-434)
- **Complexity Score**: 18
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
INSERT INTO  %%DDSTG%%.UTIL_PROS_SAP_RUN
 VALUES
 (CURRENT_DATE,1,(SEL COALESCE(COUNT(*),0) FROM  %%DDSTG%%.ACCT_REL_HLS_REME));
```

#### ❄️ Converted Snowflake SQL:
```sql
INSERT INTO %%DDSTG%%.UTIL_PROS_SAP_RUN VALUES (CURRENT_DATE, 1, (SELECT COALESCE(COUNT(*), 0) FROM %%DDSTG%%.ACCT_REL_HLS_REME))
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
INSERT INTO "%%ddstg%%"."util_pros_sap_run"
VALUES
  (
    CURRENT_DATE,
    1,
    (
      SELECT
        COALESCE(COUNT(*), 0)
      FROM "%%ddstg%%"."acct_rel_hls_reme"
    )
  )
```

#### 📊 SQL Metadata:
- **Tables**: ACCT_REL_HLS_REME, UTIL_PROS_SAP_RUN
- **Functions**: *, COUNT(*), None

### SQL Block 9 (Lines 437-488)
- **Complexity Score**: 112
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
INSERT INTO  %%STARDATADB%%.ACCT_REL
(
SUBJ_ACCT_I,
OBJC_ACCT_I,
REL_C,
EFFT_D,
EXPY_D,
STRT_D,
REL_EXPY_D,
PROS_KEY_EFFT_I,
PROS_KEY_EXPY_I,
EROR_SEQN_I,
ROW_SECU_ACCS_C,
REL_STUS_C,
SRCE_SYST_C,
SUBJ_ACCT_LEVL_N,
OBJC_ACCT_LEVL_N,
CRIN_SHRE_P,
DR_INT_SHRE_P,
RECORD_DELETED_FLAG,
CTL_ID,
PROCESS_NAME,
PROCESS_ID,
UPDATE_PROCESS_NAME,
UPDATE_PROCESS_ID
)
SEL 
SUBJ_ACCT_I,
OBJC_ACCT_I,
REL_C,
RUN_STRM_PROS_D AS EFFT_D,
'9999-12-31' AS EXPY_D,
RUN_STRM_PROS_D AS STRT_D,
NULL AS REL_EXPY_D,
(SEL MAX(PROS_KEY_EFFT_I) FROM  %%DDSTG%%.UTIL_PROS_SAP_RUN) AS PROS_KEY_EFFT_I,
NULL AS PROS_KEY_EXPY_I,
NULL AS EROR_SEQN_I,
0 AS ROW_SECU_ACCS_C,
'N' AS REL_STUS_C,
'SAP' AS SRCE_SYST_C,
SUBJ_ACCT_LEVL_N,
OBJC_ACCT_LEVL_N,
NULL AS CRIN_SHRE_P,
NULL AS DR_INT_SHRE_P,
0 AS RECORD_DELETED_FLAG,
0 AS CTL_ID,
'NON_CTLFW' AS PROCESS_NAME,
0 AS PROCESS_ID,
NULL AS UPDATE_PROCESS_NAME,
NULL AS UPDATE_PROCESS_ID
FROM  %%DDSTG%%.ACCT_REL_HLS_REME WHERE OBJC_ACCT_I LIKE 'HLS%';
```

#### ❄️ Converted Snowflake SQL:
```sql
INSERT INTO %%STARDATADB%%.ACCT_REL (SUBJ_ACCT_I, OBJC_ACCT_I, REL_C, EFFT_D, EXPY_D, STRT_D, REL_EXPY_D, PROS_KEY_EFFT_I, PROS_KEY_EXPY_I, EROR_SEQN_I, ROW_SECU_ACCS_C, REL_STUS_C, SRCE_SYST_C, SUBJ_ACCT_LEVL_N, OBJC_ACCT_LEVL_N, CRIN_SHRE_P, DR_INT_SHRE_P, RECORD_DELETED_FLAG, CTL_ID, PROCESS_NAME, PROCESS_ID, UPDATE_PROCESS_NAME, UPDATE_PROCESS_ID) SELECT SUBJ_ACCT_I, OBJC_ACCT_I, REL_C, RUN_STRM_PROS_D AS EFFT_D, '9999-12-31' AS EXPY_D, RUN_STRM_PROS_D AS STRT_D, NULL AS REL_EXPY_D, (SELECT MAX(PROS_KEY_EFFT_I) FROM %%DDSTG%%.UTIL_PROS_SAP_RUN) AS PROS_KEY_EFFT_I, NULL AS PROS_KEY_EXPY_I, NULL AS EROR_SEQN_I, 0 AS ROW_SECU_ACCS_C, 'N' AS REL_STUS_C, 'SAP' AS SRCE_SYST_C, SUBJ_ACCT_LEVL_N, OBJC_ACCT_LEVL_N, NULL AS CRIN_SHRE_P, NULL AS DR_INT_SHRE_P, 0 AS RECORD_DELETED_FLAG, 0 AS CTL_ID, 'NON_CTLFW' AS PROCESS_NAME, 0 AS PROCESS_ID, NULL AS UPDATE_PROCESS_NAME, NULL AS UPDATE_PROCESS_ID FROM %%DDSTG%%.ACCT_REL_HLS_REME WHERE OBJC_ACCT_I LIKE 'HLS%'
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
INSERT INTO "%%stardatadb%%"."acct_rel" (
  "subj_acct_i",
  "objc_acct_i",
  "rel_c",
  "efft_d",
  "expy_d",
  "strt_d",
  "rel_expy_d",
  "pros_key_efft_i",
  "pros_key_expy_i",
  "eror_seqn_i",
  "row_secu_accs_c",
  "rel_stus_c",
  "srce_syst_c",
  "subj_acct_levl_n",
  "objc_acct_levl_n",
  "crin_shre_p",
  "dr_int_shre_p",
  "record_deleted_flag",
  "ctl_id",
  "process_name",
  "process_id",
  "update_process_name",
  "update_process_id"
)
SELECT
  "acct_rel_hls_reme"."subj_acct_i" AS "subj_acct_i",
  "acct_rel_hls_reme"."objc_acct_i" AS "objc_acct_i",
  "acct_rel_hls_reme"."rel_c" AS "rel_c",
  "acct_rel_hls_reme"."run_strm_pros_d" AS "efft_d",
  '9999-12-31' AS "expy_d",
  "acct_rel_hls_reme"."run_strm_pros_d" AS "strt_d",
  NULL AS "rel_expy_d",
  (
    SELECT
      MAX("util_pros_sap_run"."pros_key_efft_i") AS "_col_0"
    FROM "%%ddstg%%"."util_pros_sap_run" AS "util_pros_sap_run"
  ) AS "pros_key_efft_i",
  NULL AS "pros_key_expy_i",
  NULL AS "eror_seqn_i",
  0 AS "row_secu_accs_c",
  'N' AS "rel_stus_c",
  'SAP' AS "srce_syst_c",
  "acct_rel_hls_reme"."subj_acct_levl_n" AS "subj_acct_levl_n",
  "acct_rel_hls_reme"."objc_acct_levl_n" AS "objc_acct_levl_n",
  NULL AS "crin_shre_p",
  NULL AS "dr_int_shre_p",
  0 AS "record_deleted_flag",
  0 AS "ctl_id",
  'NON_CTLFW' AS "process_name",
  0 AS "process_id",
  NULL AS "update_process_name",
  NULL AS "update_process_id"
FROM "%%ddstg%%"."acct_rel_hls_reme" AS "acct_rel_hls_reme"
WHERE
  "acct_rel_hls_reme"."objc_acct_i" LIKE 'HLS%'
```

#### 📊 SQL Metadata:
- **Tables**: ACCT_REL, ACCT_REL_HLS_REME, UTIL_PROS_SAP_RUN
- **Columns**: OBJC_ACCT_I, OBJC_ACCT_LEVL_N, PROS_KEY_EFFT_I, REL_C, RUN_STRM_PROS_D, SUBJ_ACCT_I, SUBJ_ACCT_LEVL_N
- **Functions**: PROS_KEY_EFFT_I

### SQL Block 10 (Lines 491-542)
- **Complexity Score**: 122
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
INSERT INTO  %%STARDATADB%%.ACCT_REL
(
SUBJ_ACCT_I,
OBJC_ACCT_I,
REL_C,
EFFT_D,
EXPY_D,
STRT_D,
REL_EXPY_D,
PROS_KEY_EFFT_I,
PROS_KEY_EXPY_I,
EROR_SEQN_I,
ROW_SECU_ACCS_C,
REL_STUS_C,
SRCE_SYST_C,
SUBJ_ACCT_LEVL_N,
OBJC_ACCT_LEVL_N,
CRIN_SHRE_P,
DR_INT_SHRE_P,
RECORD_DELETED_FLAG,
CTL_ID,
PROCESS_NAME,
PROCESS_ID,
UPDATE_PROCESS_NAME,
UPDATE_PROCESS_ID
)
SEL 
OBJC_ACCT_I AS SUBJ_ACCT_I,
SUBJ_ACCT_I AS OBJC_ACCT_I,
MIRR_REL_C AS REL_C,
RUN_STRM_PROS_D AS EFFT_D,
'9999-12-31' AS EXPY_D,
RUN_STRM_PROS_D AS STRT_D,
NULL AS REL_EXPY_D,
(SEL MAX(PROS_KEY_EFFT_I) FROM %%DDSTG%%.UTIL_PROS_SAP_RUN) AS PROS_KEY_EFFT_I,
NULL AS PROS_KEY_EXPY_I,
NULL AS EROR_SEQN_I,
0 AS ROW_SECU_ACCS_C,
'N' AS REL_STUS_C,
'SAP' AS SRCE_SYST_C,
OBJC_ACCT_LEVL_N AS SUBJ_ACCT_LEVL_N,
SUBJ_ACCT_LEVL_N AS OBJC_ACCT_LEVL_N,
NULL AS CRIN_SHRE_P,
NULL AS DR_INT_SHRE_P,
0 AS RECORD_DELETED_FLAG,
0 AS CTL_ID,
'NON_CTLFW' AS PROCESS_NAME,
0 AS PROCESS_ID,
NULL AS UPDATE_PROCESS_NAME,
NULL AS UPDATE_PROCESS_ID
FROM  %%DDSTG%%.ACCT_REL_HLS_REME WHERE OBJC_ACCT_I LIKE 'HLS%';
```

#### ❄️ Converted Snowflake SQL:
```sql
INSERT INTO %%STARDATADB%%.ACCT_REL (SUBJ_ACCT_I, OBJC_ACCT_I, REL_C, EFFT_D, EXPY_D, STRT_D, REL_EXPY_D, PROS_KEY_EFFT_I, PROS_KEY_EXPY_I, EROR_SEQN_I, ROW_SECU_ACCS_C, REL_STUS_C, SRCE_SYST_C, SUBJ_ACCT_LEVL_N, OBJC_ACCT_LEVL_N, CRIN_SHRE_P, DR_INT_SHRE_P, RECORD_DELETED_FLAG, CTL_ID, PROCESS_NAME, PROCESS_ID, UPDATE_PROCESS_NAME, UPDATE_PROCESS_ID) SELECT OBJC_ACCT_I AS SUBJ_ACCT_I, SUBJ_ACCT_I AS OBJC_ACCT_I, MIRR_REL_C AS REL_C, RUN_STRM_PROS_D AS EFFT_D, '9999-12-31' AS EXPY_D, RUN_STRM_PROS_D AS STRT_D, NULL AS REL_EXPY_D, (SELECT MAX(PROS_KEY_EFFT_I) FROM %%DDSTG%%.UTIL_PROS_SAP_RUN) AS PROS_KEY_EFFT_I, NULL AS PROS_KEY_EXPY_I, NULL AS EROR_SEQN_I, 0 AS ROW_SECU_ACCS_C, 'N' AS REL_STUS_C, 'SAP' AS SRCE_SYST_C, OBJC_ACCT_LEVL_N AS SUBJ_ACCT_LEVL_N, SUBJ_ACCT_LEVL_N AS OBJC_ACCT_LEVL_N, NULL AS CRIN_SHRE_P, NULL AS DR_INT_SHRE_P, 0 AS RECORD_DELETED_FLAG, 0 AS CTL_ID, 'NON_CTLFW' AS PROCESS_NAME, 0 AS PROCESS_ID, NULL AS UPDATE_PROCESS_NAME, NULL AS UPDATE_PROCESS_ID FROM %%DDSTG%%.ACCT_REL_HLS_REME WHERE OBJC_ACCT_I LIKE 'HLS%'
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
INSERT INTO "%%stardatadb%%"."acct_rel" (
  "subj_acct_i",
  "objc_acct_i",
  "rel_c",
  "efft_d",
  "expy_d",
  "strt_d",
  "rel_expy_d",
  "pros_key_efft_i",
  "pros_key_expy_i",
  "eror_seqn_i",
  "row_secu_accs_c",
  "rel_stus_c",
  "srce_syst_c",
  "subj_acct_levl_n",
  "objc_acct_levl_n",
  "crin_shre_p",
  "dr_int_shre_p",
  "record_deleted_flag",
  "ctl_id",
  "process_name",
  "process_id",
  "update_process_name",
  "update_process_id"
)
SELECT
  "acct_rel_hls_reme"."objc_acct_i" AS "subj_acct_i",
  "acct_rel_hls_reme"."objc_acct_i" AS "objc_acct_i",
  "acct_rel_hls_reme"."mirr_rel_c" AS "rel_c",
  "acct_rel_hls_reme"."run_strm_pros_d" AS "efft_d",
  '9999-12-31' AS "expy_d",
  "acct_rel_hls_reme"."run_strm_pros_d" AS "strt_d",
  NULL AS "rel_expy_d",
  (
    SELECT
      MAX("util_pros_sap_run"."pros_key_efft_i") AS "_col_0"
    FROM "%%ddstg%%"."util_pros_sap_run" AS "util_pros_sap_run"
  ) AS "pros_key_efft_i",
  NULL AS "pros_key_expy_i",
  NULL AS "eror_seqn_i",
  0 AS "row_secu_accs_c",
  'N' AS "rel_stus_c",
  'SAP' AS "srce_syst_c",
  "acct_rel_hls_reme"."objc_acct_levl_n" AS "subj_acct_levl_n",
  "acct_rel_hls_reme"."objc_acct_levl_n" AS "objc_acct_levl_n",
  NULL AS "crin_shre_p",
  NULL AS "dr_int_shre_p",
  0 AS "record_deleted_flag",
  0 AS "ctl_id",
  'NON_CTLFW' AS "process_name",
  0 AS "process_id",
  NULL AS "update_process_name",
  NULL AS "update_process_id"
FROM "%%ddstg%%"."acct_rel_hls_reme" AS "acct_rel_hls_reme"
WHERE
  "acct_rel_hls_reme"."objc_acct_i" LIKE 'HLS%'
```

#### 📊 SQL Metadata:
- **Tables**: ACCT_REL, ACCT_REL_HLS_REME, UTIL_PROS_SAP_RUN
- **Columns**: MIRR_REL_C, OBJC_ACCT_I, OBJC_ACCT_LEVL_N, PROS_KEY_EFFT_I, RUN_STRM_PROS_D, SUBJ_ACCT_I, SUBJ_ACCT_LEVL_N
- **Functions**: PROS_KEY_EFFT_I

### SQL Block 11 (Lines 549-601)
- **Complexity Score**: 175
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
INSERT INTO %%STARDATADB%%.UTIL_PROS_ISAC
SEL
PROS_KEY_EFFT_I AS PROS_KEY_I
,'C4182118' AS CONV_M
,'MNL' AS CONV_TYPE_M
,CURRENT_TIMESTAMP(0) AS PROS_RQST_S
,NULL AS PROS_LAST_RQST_S
,NULL AS PROS_RQST_Q
,NULL AS BTCH_RUN_D
,NULL AS BTCH_KEY_I
,NULL AS SRCE_SYST_M
,NULL AS SRCE_M
,'ACCT_REL' AS TRGT_M
,NULL AS SUCC_F
,NULL AS COMT_F
,NULL AS COMT_S
,NULL AS MLTI_LOAD_EFFT_D
,NULL AS SYST_S
,NULL AS MLTI_LOAD_COMT_S
,NULL AS SYST_ET_Q
,NULL AS SYST_UV_Q
,RECD_CNT * 2 AS SYST_INS_Q
,NULL AS SYST_UPD_Q
,NULL AS SYST_DEL_Q
,NULL AS SYST_ET_TABL_M
,NULL AS SYST_UV_TABL_M
,NULL AS SYST_HEAD_ET_TABL_M
,NULL AS SYST_HEAD_UV_TABL_M
,NULL AS SYST_TRLR_ET_TABL_M
,NULL AS SYST_TRLR_UV_TABL_M
,NULL AS PREV_PROS_KEY_I
,NULL AS HEAD_RECD_TYPE_C
,NULL AS HEAD_FILE_M
,NULL AS HEAD_BTCH_RUN_D
,NULL AS HEAD_FILE_CRAT_S
,NULL AS HEAD_GENR_PRGM_M
,NULL AS HEAD_BTCH_KEY_I
,NULL AS HEAD_PROS_KEY_I
,NULL AS HEAD_PROS_PREV_KEY_I
,NULL AS TRLR_RECD_TYPE_C
,NULL AS TRLR_RECD_Q
,NULL AS TRLR_HASH_TOTL_A
,NULL AS TRLR_COLM_HASH_TOTL_M
,NULL AS TRLR_EROR_RECD_Q
,NULL AS TRLR_FILE_COMT_S
,NULL AS TRLR_RECD_ISRT_Q
,NULL AS TRLR_RECD_UPDT_Q
,NULL AS TRLR_RECD_DELT_Q
FROM
 %%DDSTG%%.UTIL_PROS_SAP_RUN
WHERE RUN_DATE=CURRENT_DATE AND 
PROS_KEY_EFFT_I = (SEL MAX(PROS_KEY_EFFT_I) FROM %%DDSTG%%.UTIL_PROS_SAP_RUN WHERE RUN_DATE=CURRENT_DATE);
```

#### ❄️ Converted Snowflake SQL:
```sql
INSERT INTO %%STARDATADB%%.UTIL_PROS_ISAC SELECT PROS_KEY_EFFT_I AS PROS_KEY_I, 'C4182118' AS CONV_M, 'MNL' AS CONV_TYPE_M, CURRENT_TIMESTAMP(0) AS PROS_RQST_S, NULL AS PROS_LAST_RQST_S, NULL AS PROS_RQST_Q, NULL AS BTCH_RUN_D, NULL AS BTCH_KEY_I, NULL AS SRCE_SYST_M, NULL AS SRCE_M, 'ACCT_REL' AS TRGT_M, NULL AS SUCC_F, NULL AS COMT_F, NULL AS COMT_S, NULL AS MLTI_LOAD_EFFT_D, NULL AS SYST_S, NULL AS MLTI_LOAD_COMT_S, NULL AS SYST_ET_Q, NULL AS SYST_UV_Q, RECD_CNT * 2 AS SYST_INS_Q, NULL AS SYST_UPD_Q, NULL AS SYST_DEL_Q, NULL AS SYST_ET_TABL_M, NULL AS SYST_UV_TABL_M, NULL AS SYST_HEAD_ET_TABL_M, NULL AS SYST_HEAD_UV_TABL_M, NULL AS SYST_TRLR_ET_TABL_M, NULL AS SYST_TRLR_UV_TABL_M, NULL AS PREV_PROS_KEY_I, NULL AS HEAD_RECD_TYPE_C, NULL AS HEAD_FILE_M, NULL AS HEAD_BTCH_RUN_D, NULL AS HEAD_FILE_CRAT_S, NULL AS HEAD_GENR_PRGM_M, NULL AS HEAD_BTCH_KEY_I, NULL AS HEAD_PROS_KEY_I, NULL AS HEAD_PROS_PREV_KEY_I, NULL AS TRLR_RECD_TYPE_C, NULL AS TRLR_RECD_Q, NULL AS TRLR_HASH_TOTL_A, NULL AS TRLR_COLM_HASH_TOTL_M, NULL AS TRLR_EROR_RECD_Q, NULL AS TRLR_FILE_COMT_S, NULL AS TRLR_RECD_ISRT_Q, NULL AS TRLR_RECD_UPDT_Q, NULL AS TRLR_RECD_DELT_Q FROM %%DDSTG%%.UTIL_PROS_SAP_RUN WHERE RUN_DATE = CURRENT_DATE AND PROS_KEY_EFFT_I = (SELECT MAX(PROS_KEY_EFFT_I) FROM %%DDSTG%%.UTIL_PROS_SAP_RUN WHERE RUN_DATE = CURRENT_DATE)
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
INSERT INTO "%%stardatadb%%"."util_pros_isac"
WITH "_u_0" AS (
  SELECT
    MAX("util_pros_sap_run"."pros_key_efft_i") AS "_col_0"
  FROM "%%ddstg%%"."util_pros_sap_run" AS "util_pros_sap_run"
  WHERE
    "util_pros_sap_run"."run_date" = CURRENT_DATE
)
SELECT
  "util_pros_sap_run"."pros_key_efft_i" AS "pros_key_i",
  'C4182118' AS "conv_m",
  'MNL' AS "conv_type_m",
  CURRENT_TIMESTAMP(0) AS "pros_rqst_s",
  NULL AS "pros_last_rqst_s",
  NULL AS "pros_rqst_q",
  NULL AS "btch_run_d",
  NULL AS "btch_key_i",
  NULL AS "srce_syst_m",
  NULL AS "srce_m",
  'ACCT_REL' AS "trgt_m",
  NULL AS "succ_f",
  NULL AS "comt_f",
  NULL AS "comt_s",
  NULL AS "mlti_load_efft_d",
  NULL AS "syst_s",
  NULL AS "mlti_load_comt_s",
  NULL AS "syst_et_q",
  NULL AS "syst_uv_q",
  "util_pros_sap_run"."recd_cnt" * 2 AS "syst_ins_q",
  NULL AS "syst_upd_q",
  NULL AS "syst_del_q",
  NULL AS "syst_et_tabl_m",
  NULL AS "syst_uv_tabl_m",
  NULL AS "syst_head_et_tabl_m",
  NULL AS "syst_head_uv_tabl_m",
  NULL AS "syst_trlr_et_tabl_m",
  NULL AS "syst_trlr_uv_tabl_m",
  NULL AS "prev_pros_key_i",
  NULL AS "head_recd_type_c",
  NULL AS "head_file_m",
  NULL AS "head_btch_run_d",
  NULL AS "head_file_crat_s",
  NULL AS "head_genr_prgm_m",
  NULL AS "head_btch_key_i",
  NULL AS "head_pros_key_i",
  NULL AS "head_pros_prev_key_i",
  NULL AS "trlr_recd_type_c",
  NULL AS "trlr_recd_q",
  NULL AS "trlr_hash_totl_a",
  NULL AS "trlr_colm_hash_totl_m",
  NULL AS "trlr_eror_recd_q",
  NULL AS "trlr_file_comt_s",
  NULL AS "trlr_recd_isrt_q",
  NULL AS "trlr_recd_updt_q",
  NULL AS "trlr_recd_delt_q"
FROM "%%ddstg%%"."util_pros_sap_run" AS "util_pros_sap_run"
JOIN "_u_0" AS "_u_0"
  ON "_u_0"."_col_0" = "util_pros_sap_run"."pros_key_efft_i"
WHERE
  "util_pros_sap_run"."run_date" = CURRENT_DATE
```

#### 📊 SQL Metadata:
- **Tables**: UTIL_PROS_ISAC, UTIL_PROS_SAP_RUN
- **Columns**: PROS_KEY_EFFT_I, RECD_CNT, RUN_DATE
- **Functions**: 0, None, PROS_KEY_EFFT_I, RUN_DATE = CURRENT_DATE

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
