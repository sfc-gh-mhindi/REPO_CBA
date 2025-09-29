.RUN FILE=${ETL_APP_BTEQ}/bteq_login.sql
.IF ERRORCODE <> 0 THEN .GOTO EXITERR


.SET QUIET OFF
.SET ECHOREQ ON
.SET FORMAT OFF
.SET WIDTH 120
----------------------------------------------------------------------
-- $LastChangedBy: vajapes $
-- $LastChangedDate: 2012-02-28 09:09:44 +1100 (Tue, 28 Feb 2012) $
-- $LastChangedRevision: 9225 $
----------------------------------------------------------------------
------------------------------------------------------------------------------
--
--  Description :Loading ACCT_I that need to be adjusted into staging area
--
--   Ver  Date       Modified By            Description
--  ---- ---------- ---------------------- -----------------------------------
--  1.0  2011-10-05 Suresh Vajapeyajula               Initial Version
------------------------------------------------------------------------------

/*Delete the previous load from the staging table and populate 
current balances that need to be adjusted*/

DELETE PDDSTG.ACCT_BALN_BKDT_STG1;

.IF ERRORCODE <> 0 THEN .GOTO EXITERR

INSERT	INTO  PDDSTG.ACCT_BALN_BKDT_STG1
(
ACCT_I,
BALN_TYPE_C,
CALC_FUNC_C,
TIME_PERD_C,
BKDT_EFFT_D,
BKDT_EXPY_D,
BALN_A,
CALC_F,
PROS_KEY_EFFT_I,
PROS_KEY_EXPY_I,
BKDT_PROS_KEY_I,
ORIG_SRCE_SYST_C,
SRCE_SYST_C,
LOAD_D
)
SELECT	
A.ACCT_I,
A.BALN_TYPE_C,
A.CALC_FUNC_C,                  
A.TIME_PERD_C,                  
A.BKDT_EFFT_D,
A.BKDT_EXPY_D,
A.BALN_A,
A.CALC_F,
A.PROS_KEY_EFFT_I,
A.PROS_KEY_EXPY_I,
A.BKDT_PROS_KEY_I,
A.ORIG_SRCE_SYST_C,
A.SRCE_SYST_C,
A.LOAD_D
FROM	
PVTECH.ACCT_BALN_BKDT  A
INNER JOIN
/*Identify suitable candidates for processing. 
This avoids pulling entire history into staging environment*/
(
SELECT	
ACCT_I, 
MIN(BKDT_ADJ_FROM_D) AS BKDT_ADJ_FROM_D
FROM	
PDDSTG.ACCT_BALN_BKDT_ADJ_RULE
GROUP	BY 1
)B
ON	
A.ACCT_I=B.ACCT_I
WHERE	
A.BKDT_EXPY_D >= B.BKDT_ADJ_FROM_D
;

.IF ERRORCODE <> 0 THEN .GOTO EXITERR


--Delete previous day load and Insert current day load after applying the logic

DELETE	 PDDSTG.ACCT_BALN_BKDT_STG2;

.IF ERRORCODE <> 0 THEN .GOTO EXITERR

INSERT	INTO   PDDSTG.ACCT_BALN_BKDT_STG2
SELECT DISTINCT	
DT1.ACCT_I       AS ACCT_I
/*The BALN_TYPE_C in ACCT_BALN_BKDT table is hardcoded to 'BDCL'*/
,COALESCE(B.BALN_TYPE_C,'BDCL') 
,COALESCE(B.CALC_FUNC_C,'SPOT')              
,COALESCE(B.TIME_PERD_C,'E')
,DT1.BKDT_EFFT_D AS BKDT_EFFT_D
,DT1.BKDT_EXPY_D AS BKDT_EXPY_D
/* Calculate the adjusted balance value as the sum of all relevant adjustments plus the relevant balance value */
/* Note that MAX is used for BAL amount simply to identify the single balance valid during the time period */
,MAX(COALESCE(B.BALN_A,0.0)) OVER 
(PARTITION BY DT1.ACCT_I,DT1.BKDT_EFFT_D,DT1.BKDT_EXPY_D) 
+ SUM(COALESCE(A.ADJ_A,0.0)) OVER 
(PARTITION BY DT1.ACCT_I, DT1.BKDT_EFFT_D,DT1.BKDT_EXPY_D)    AS BALN_A
,COALESCE(B.CALC_F,'N')
,B.PROS_KEY_EFFT_I
,B.PROS_KEY_EXPY_I
,PKEY.BKDT_PROS_KEY_I AS BKDT_PROS_KEY_I
,A.PROS_KEY_EFFT_I AS ADJ_PROS_KEY_EFFT_I
,COALESCE(B.ORIG_SRCE_SYST_C,'SAP')
,COALESCE(B.SRCE_SYST_C,'GDW')
,CURRENT_DATE AS LOAD_D
/*  Identify ALL TIME periods OF interest FOR EACH ACCT_I */
FROM	
	   (SELECT	DT0.ACCT_I
	   /* Calculate start points of time periods pf interest based on end point of previous time period */
      ,(MAX(DT0.BKDT_EXPY_D) OVER (PARTITION BY DT0.ACCT_I ORDER
      	BY DT0.BKDT_EXPY_D ROWS BETWEEN 1 PRECEDING 
      	AND	1 PRECEDING) + 1)  AS BKDT_EFFT_D  , DT0.BKDT_EXPY_D
      	FROM	 (
      	/*  Identify END points OF ALL TIME periods OF interest FOR EACH  ACCT_I*/	               
		SELECT ACCT_I, BKDT_EXPY_D          
		FROM PDDSTG.ACCT_BALN_BKDT_STG1
		UNION	 	            
		SELECT ACCT_I,ADJ_TO_D            
		FROM PDDSTG.ACCT_BALN_BKDT_ADJ_RULE
		UNION	                
		SELECT	ACCT_I,BKDT_EFFT_D - 1                
		FROM	PDDSTG.ACCT_BALN_BKDT_STG1
		UNION	                
		SELECT	 ACCT_I,BKDT_ADJ_FROM_D - 1             
		FROM	PDDSTG.ACCT_BALN_BKDT_ADJ_RULE) DT0 (ACCT_I,BKDT_EXPY_D)
		/* Ignore record where there is no start point */
		QUALIFY	BKDT_EFFT_D IS NOT NULL
	) DT1
 /* Join to balance table based on ACCT_I and intersection with time periods of interest */
/* Note that there may be no balance amount related to a time period */
			
LEFT OUTER JOIN
PDDSTG.ACCT_BALN_BKDT_STG1  B
ON DT1.ACCT_I = B.ACCT_I
AND(
(DT1.BKDT_EFFT_D, DT1.BKDT_EXPY_D) OVERLAPS (B.BKDT_EFFT_D,B.BKDT_EXPY_D)
/* as Overlaps does not include equality*/
OR DT1.BKDT_EFFT_D = B.BKDT_EFFT_D
OR DT1.BKDT_EXPY_D = B.BKDT_EXPY_D
)
/* Join to adjustment table based on ACCT_I and intersection with time periods of interest */
/* Note that there may be multiple adjustments or no adjustments related to a time period */

LEFT OUTER JOIN
PDDSTG.ACCT_BALN_BKDT_ADJ_RULE A
ON DT1.ACCT_I = A.ACCT_I                   
AND (
(DT1.BKDT_EFFT_D, DT1.BKDT_EXPY_D) OVERLAPS (A.BKDT_ADJ_FROM_D,A.ADJ_TO_D)
OR DT1.BKDT_EFFT_D = A.BKDT_ADJ_FROM_D
OR DT1.BKDT_EXPY_D = A.ADJ_TO_D)
CROSS JOIN
/*update the latest PROS_KEY_I into subsequent inserts*/
(SELECT MAX(PROS_KEY_I)  AS BKDT_PROS_KEY_I
FROM PVTECH.UTIL_PROS_ISAC WHERE 
CONV_M='CAD_X01_ACCT_BALN_BKDT')PKEY;

.IF ERRORCODE <> 0 THEN .GOTO EXITERR

.QUIT 0
.LOGOFF
.EXIT


.LABEL EXITERR
.QUIT ERRORCODE
.LOGOFF
.EXIT