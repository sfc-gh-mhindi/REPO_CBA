-- Snowflake Views converted from Teradata
-- Generated automatically with dependency ordering
USE ROLE ACCOUNTADMIN;
USE DATABASE PS_GDW1_BTEQ;

-- drop SCHEMA IF EXISTS PS_GDW1_BTEQ.D_D04_V_COX_001_STD_0;
-- drop SCHEMA IF EXISTS PS_GDW1_BTEQ.JELIFFT;
-- -- drop SCHEMA IF EXISTS PS_GDW1_BTEQ.PVDATA;
-- -- drop SCHEMA IF EXISTS PS_GDW1_BTEQ.PVTECH;
-- drop SCHEMA IF EXISTS PS_GDW1_BTEQ.P_P01_PVTECH;
-- drop SCHEMA IF EXISTS PS_GDW1_BTEQ.P_P01_V_COX_001_STD_0;
-- drop SCHEMA IF EXISTS PS_GDW1_BTEQ.P_V_COX_001_STD_0;

-- ===============================
-- VIEWS IN DEPENDENCY ORDER
-- ===============================

--  1. PVTECH.DERV_PRTF_INT_HIST_PSST
CREATE OR REPLACE VIEW PS_GDW1_BTEQ.PVTECH.DERV_PRTF_INT_HIST_PSST AS
SELECT
INT_GRUP_I                    
,INT_GRUP_TYPE_C               
,EFFT_D                        
,EXPY_D                        
,VALD_FROM_D                   
,VALD_TO_D                     
,JOIN_FROM_D                   
,JOIN_TO_D                                            
,PTCL_N                        
,REL_MNGE_I                             
FROM
  PS_CLD_RW.STARCADPRODDATA.DERV_PRTF_INT_HIST_PSST
-- $LastChangedBy: jelifft $
-- $LastChangedDate: 2013-07-24 11:07:50 +1000 (Wed, 24 Jul 2013) $
-- $LastChangedRevision: 12357 $
;


-- STAR_CAD_PROD_DATA.DERV_PRTF_INT_PSST -> PVTECH.DERV_PRTF_INT_PSST

/* <sc-view> PS_GDW1_BTEQ.PVTECH.DERV_PRTF_INT_PSST </sc-view> */
/* Re-place the existing views */


--  2. PVTECH.DERV_PRTF_PATY_INT_GRUP_PSST
CREATE OR REPLACE VIEW PS_GDW1_BTEQ.PVTECH.DERV_PRTF_PATY_INT_GRUP_PSST AS
SELECT
   INT_GRUP_I                    
  ,PATY_I                        
  ,EFFT_D                        
  ,EXPY_D                        
  ,VALD_FROM_D                   
  ,VALD_TO_D                     
  ,PERD_D                        
  ,REL_C                         
  ,SRCE_SYST_C 
  ,ROW_N
  ,ROW_SECU_ACCS_C               
  ,PROS_KEY_I                    
FROM
  PS_CLD_RW.STARCADPRODDATA.DERV_PRTF_PATY_INT_GRUP_PSST
-- $LastChangedBy: jelifft $
-- $LastChangedDate: 2013-07-24 11:07:50 +1000 (Wed, 24 Jul 2013) $
-- $LastChangedRevision: 12357 $
;


-- STAR_CAD_PROD_DATA.DERV_PRTF_PATY_OWN_REL -> PVTECH.DERV_PRTF_PATY_OWN_REL

/* <sc-view> PS_GDW1_BTEQ.PVTECH.DERV_PRTF_PATY_OWN_REL </sc-view> */



 

--  3. PVTECH.ACCT_BALN_BKDT_AUDT
CREATE OR REPLACE VIEW PS_GDW1_BTEQ.PVTECH.ACCT_BALN_BKDT_AUDT
(
ACCT_I,
BALN_TYPE_C,
CALC_FUNC_C,
TIME_PERD_C,
BALN_A,
CALC_F,
SRCE_SYST_C,
ORIG_SRCE_SYST_C,
LOAD_D,
BKDT_EFFT_D,
BKDT_EXPY_D,
PROS_KEY_EFFT_I,
ABAL_PROS_KEY_EFFT_I,
ABAL_PROS_KEY_EXPY_I,
ABAL_BKDT_PROS_KEY_I,
ADJ_PROS_KEY_EFFT_I
) AS
SELECT
ACCT_I,
BALN_TYPE_C,
CALC_FUNC_C,
TIME_PERD_C,
BALN_A,
CALC_F,
SRCE_SYST_C,
ORIG_SRCE_SYST_C,
LOAD_D,
BKDT_EFFT_D,
BKDT_EXPY_D,
PROS_KEY_EFFT_I,
ABAL_PROS_KEY_EFFT_I,
ABAL_PROS_KEY_EXPY_I,
ABAL_BKDT_PROS_KEY_I , 
ADJ_PROS_KEY_EFFT_I
FROM
PS_CLD_RW.STARCADPRODDATA.ACCT_BALN_BKDT_AUDT;

-- PS_CLD_RW.STARCADPRODDATA.ACCT_PATY_TAX_INSS -> PS_GDW1_BTEQ.P_P01_PVTECH.ACCT_PATY_TAX_INSS
/* <sc-view> PS_GDW1_BTEQ.P_P01_PVTECH.ACCT_PATY_TAX_INSS </sc-view> */




--  4. PVTECH.ACCT_BALN_BKDT (depends on: PVSECURITY.ROW_LEVL_SECU_USER_PRFL)
CREATE OR REPLACE VIEW PS_GDW1_BTEQ.PVTECH.ACCT_BALN_BKDT
(
        ACCT_I,
  BALN_TYPE_C,
  CALC_FUNC_C,
  TIME_PERD_C,
  BALN_A,
  CALC_F,
  SRCE_SYST_C,
  ORIG_SRCE_SYST_C,
  LOAD_D,
  BKDT_EFFT_D,
  BKDT_EXPY_D,
  PROS_KEY_EFFT_I,
  PROS_KEY_EXPY_I,
  BKDT_PROS_KEY_I,
  CNCY_C,
  ROW_SECU_ACCS_C
) AS
SELECT
        ACCT_I,
  BALN_TYPE_C,
  CALC_FUNC_C,
  TIME_PERD_C,
  BALN_A,
  CALC_F,
  SRCE_SYST_C,
  ORIG_SRCE_SYST_C,
  LOAD_D,
  BKDT_EFFT_D,
  BKDT_EXPY_D,
  PROS_KEY_EFFT_I,
  PROS_KEY_EXPY_I,
  BKDT_PROS_KEY_I,
  CNCY_C,
  ROW_SECU_ACCS_C
  FROM
        PS_CLD_RW.STARCADPRODDATA.ACCT_BALN_BKDT
 
 WHERE ( 
(
/* Start - RLS */
COALESCE(ROW_SECU_ACCS_C,0) = 0 OR
GETBIT( (
        SELECT
                    try_to_number(ROW_SECU_PRFL_C::varchar)  --Need to double check this logic
        FROM
                    PS_GDW1_BTEQ.PVSECURITY.ROW_LEVL_SECU_USER_PRFL
        WHERE
                    UPPER(RTRIM( username)) = UPPER(RTRIM(CURRENT_USER))
        ),ROW_SECU_ACCS_C
        ) = 1
/* End - RLS */
)
    );


-- STAR_CAD_PROD_DATA.ACCT_BALN_BKDT_AUDT -> PVDATA.ACCT_BALN_BKDT_AUDT

/* <sc-view> PVDATA.ACCT_BALN_BKDT_AUDT </sc-view> */

--  5. PVTECH.ACCT_INT_GRUP (depends on: PVSECURITY.ROW_LEVL_SECU_USER_PRFL)
CREATE OR REPLACE VIEW PS_GDW1_BTEQ.PVTECH.ACCT_INT_GRUP
(
        ACCT_I,
        INT_GRUP_I,
        REL_C,
        SRCE_SYST_C,
        VALD_FROM_D,
        VALD_TO_D,
        EFFT_D,
        EXPY_D,
        PROS_KEY_EFFT_I,
        PROS_KEY_EXPY_I,
        ROW_SECU_ACCS_C
) AS
SELECT
        ACCT_I,
        INT_GRUP_I,
        REL_C,
        SRCE_SYST_C,
        VALD_FROM_D,
        VALD_TO_D,
        EFFT_D,
        EXPY_D,
        PROS_KEY_EFFT_I,
        PROS_KEY_EXPY_I,
        ROW_SECU_ACCS_C
  FROM
        PS_CLD_RW.STARCADPRODDATA.ACCT_INT_GRUP
 WHERE
(
/* Start - RLS */
COALESCE(ROW_SECU_ACCS_C,0) = 0 OR
GETBIT( (
        SELECT
                    try_to_number(ROW_SECU_PRFL_C::varchar)  --Need to double check this logic
        FROM
                    PS_GDW1_BTEQ.PVSECURITY.ROW_LEVL_SECU_USER_PRFL
        WHERE
                    UPPER(RTRIM( username)) = UPPER(RTRIM(CURRENT_USER))
        ),ROW_SECU_ACCS_C
        ) = 1
/* End - RLS */
);


-- PVTECH.DERV_PRTF_ACCT_HIST_PSST -> PVTECH.DERV_PRTF_ACCT_HIST_PSST

/* <sc-view> PS_GDW1_BTEQ.PVTECH.DERV_PRTF_ACCT_HIST_PSST </sc-view> */


--  6. P_P01_PVTECH.ACCT_INT_GRUP
-- CREATE VIEW PS_GDW1_BTEQ.P_P01_PVTECH.ACCT_INT_GRUP
-- (
--         ACCT_I,
--         INT_GRUP_I,
--         REL_C,
--         SRCE_SYST_C,
--         VALD_FROM_D,
--         VALD_TO_D,
--         EFFT_D,
--         EXPY_D,
--         PROS_KEY_EFFT_I,
--         PROS_KEY_EXPY_I,
--         ROW_SECU_ACCS_C
-- ) AS
-- SELECT
--         ACCT_I,
--         INT_GRUP_I,
--         REL_C,
--         SRCE_SYST_C,
--         VALD_FROM_D,
--         VALD_TO_D,
--         EFFT_D,
--         EXPY_D,
--         PROS_KEY_EFFT_I,
--         PROS_KEY_EXPY_I,
--         ROW_SECU_ACCS_C
--   FROM
--         PS_CLD_RW.PP01STARCADPRODDATA.ACCT_INT_GRUP
--  WHERE
-- -- if row_secu_accs_c = 0 then the row can be returned
--         ROW_SECU_ACCS_C = 0
--     OR
-- -- the following case returns 1 if the row can be read and 0 if the
-- -- row cannot be read
-- -- the return value from the case is tested against 1 to allow the
-- -- row to be returned
-- -- currently this is restricted to profile values from 0 to 512
-- -- this is because we store the profile as a character
-- -- representation of the number in the three characters starting at
-- -- position 2
-- -- this allows a max value for the profile of 999 and that can be
-- -- accomodated in 10 binary bits which equates to 512 - hence the
-- -- upper limit

-- -- please note that the sequence of tests has been determined to
-- -- give the best response to the widest set of users

-- -- test the return from the case against the value 1
--         1 = CASE

-- -- test  1 - if past 0 then no point in continuing if the profile is
-- --           null
--                 WHEN
--                         PROFILE IS NULL
--                 THEN
--                         0

-- -- test  2 - if past 0 then no point in continuing if the profile = 0
--                 WHEN
--                         CAST (SUBSTRING(PROFILE,2,3) AS INTEGER) = 0
--                 THEN
--                         0

-- -- test  3 - if row_secu_accs_c and profiles are equal then the row
-- --           can be returned
--                 WHEN
--                         ROW_SECU_ACCS_C = CAST (SUBSTRING(PROFILE,2,3) AS INTEGER)
--                 THEN
--                         1

-- -- test  4 - if the first bit is set on both row_secu_accs_c & the
-- --           profile then the row can be returned
--                 WHEN
--                         CAST (SUBSTRING(PROFILE,2,3) AS INTEGER) % 2 = 1
--                  AND
--                         ROW_SECU_ACCS_C % 2 = 1
--                 THEN
--                         1

-- -- test  5 - if past 1 then no point in continuing if either the
-- --           row_secu_accs_c or the profile < 2
--                 WHEN
--                         CAST (SUBSTRING(PROFILE,2,3) AS INTEGER) < 2
--                   OR
--                         ROW_SECU_ACCS_C < 2
--                 THEN
--                         0

-- -- test  6 - if the second bit is set on both row_secu_accs_c & the
-- --           profile then the row can be returned
--                 WHEN
--                         (CAST (SUBSTRING(PROFILE,2,3) AS INTEGER)/2) % 2 = 1
--                  AND
--                         (ROW_SECU_ACCS_C/2) % 2 = 1
--                 THEN
--                         1

-- -- test  7 - if past 2 then no point in continuing if either the
-- --           row_secu_accs_c or the profile < 4
--                 WHEN
--                         CAST (SUBSTRING(PROFILE,2,3) AS INTEGER) < 4
--                   OR
--                         ROW_SECU_ACCS_C < 4
--                 THEN
--                         0

-- -- test  8 - if the third bit is set on both row_secu_accs_c & the
-- --           profile then the row can be returned
--                 WHEN
--                         (CAST (SUBSTRING(PROFILE,2,3) AS INTEGER)/4) % 2 = 1
--                  AND
--                         (ROW_SECU_ACCS_C/4) % 2 = 1
--                 THEN
--                         1

-- -- test  9 - if past 4 then no point in continuing if either the
-- --           row_secu_accs_c or the profile < 8
--                 WHEN
--                         CAST (SUBSTRING(PROFILE,2,3) AS INTEGER) < 8
--                   OR
--                         ROW_SECU_ACCS_C < 8
--                 THEN
--                         0

-- -- test 10 - if the fourth bit is set on both row_secu_accs_c & the
-- --           profile then the row can be returned
--                 WHEN
--                         (CAST (SUBSTRING(PROFILE,2,3) AS INTEGER)/8) % 2 = 1
--                  AND
--                         (ROW_SECU_ACCS_C/8) % 2 = 1
--                 THEN
--                         1

-- -- test 11 - if past 8 then no point in continuing if either the
-- --           row_secu_accs_c or the profile < 16
--                 WHEN
--                         CAST (SUBSTRING(PROFILE,2,3) AS INTEGER) < 16
--                   OR
--                         ROW_SECU_ACCS_C < 16
--                 THEN
--                         0

-- -- test 12 - if the fifth bit is set on both row_secu_accs_c & the
-- --           profile then the row can be returned
--                 WHEN
--                         (CAST (SUBSTRING(PROFILE,2,3) AS INTEGER)/16) % 2 = 1
--                  AND
--                         (ROW_SECU_ACCS_C/16) % 2 = 1
--                 THEN
--                         1

-- -- test 13 - if past 16 then no point in continuing if either the
-- --           row_secu_accs_c or the profile < 32
--                 WHEN
--                         CAST (SUBSTRING(PROFILE,2,3) AS INTEGER) < 32
--                   OR
--                         ROW_SECU_ACCS_C < 32
--                 THEN
--                         0

-- -- test 14 - if the sixth bit is set on both row_secu_accs_c & the
-- --           profile then the row can be returned
--                 WHEN
--                         (CAST (SUBSTRING(PROFILE,2,3) AS INTEGER)/32) % 2 = 1
--                  AND
--                         (ROW_SECU_ACCS_C/32) % 2 = 1
--                 THEN
--                         1

-- -- test 15 - if past 32 then no point in continuing if either the
-- --           row_secu_accs_c or the profile < 64
--                 WHEN
--                         CAST (SUBSTRING(PROFILE,2,3) AS INTEGER) < 64
--                   OR
--                         ROW_SECU_ACCS_C < 64
--                 THEN
--                         0

-- -- test 16 - if the seventh bit is set on both row_secu_accs_c & the
-- --           profile then the row can be returned
--                 WHEN
--                         (CAST (SUBSTRING(PROFILE,2,3) AS INTEGER)/64) % 2 = 1
--                  AND
--                         (ROW_SECU_ACCS_C/64) % 2 = 1
--                 THEN
--                         1

-- -- test 17 - if past 64 then no point in continuing if either the
-- --           row_secu_accs_c or the profile < 128
--                 WHEN
--                         CAST (SUBSTRING(PROFILE,2,3) AS INTEGER) < 128
--                   OR
--                         ROW_SECU_ACCS_C < 128
--                 THEN
--                         0

-- -- test 18 - if the eighth bit is set on both row_secu_accs_c & the
-- --           profile then the row can be returned
--                 WHEN
--                         (CAST (SUBSTRING(PROFILE,2,3) AS INTEGER)/128) % 2 = 1
--                  AND
--                         (ROW_SECU_ACCS_C/128) % 2 = 1
--                 THEN
--                         1

-- -- test 19 - if past 128 then no point in continuing if either the
-- --           row_secu_accs_c or the profile < 256
--                 WHEN
--                         CAST (SUBSTRING(PROFILE,2,3) AS INTEGER) < 256
--                   OR
--                         ROW_SECU_ACCS_C < 256
--                 THEN
--                         0

-- -- test 20 - if the ninth bit is set on both row_secu_accs_c & the
-- --           profile then the row can be returned
--                 WHEN
--                         (CAST (SUBSTRING(PROFILE,2,3) AS INTEGER)/256) % 2 = 1
--                  AND
--                         (ROW_SECU_ACCS_C/256) % 2 = 1
--                 THEN
--                         1

-- -- test 21 - if past 256 then no point in continuing if either the
-- --           row_secu_accs_c or the profile < 512
--                 WHEN
--                         CAST (SUBSTRING(PROFILE,2,3) AS INTEGER) < 512
--                   OR
--                         ROW_SECU_ACCS_C < 512
--                 THEN
--                         0

-- -- test 22 - if the tenth bit is set on both row_secu_accs_c & the
-- --           profile then the row can be returned
--                 WHEN
--                         (CAST (SUBSTRING(PROFILE,2,3) AS INTEGER)/512) % 2 = 1
--                  AND
--                         (ROW_SECU_ACCS_C/512) % 2 = 1
--                 THEN
--                         1

-- -- otherwise the row cannot be read
--                 ELSE
--                         0

--         END;


-- PVTECH.ACCT_INT_GRUP -> PVTECH.ACCT_INT_GRUP

/* <sc-view> PS_GDW1_BTEQ.PVTECH.ACCT_INT_GRUP </sc-view> */



 

--  7. D_D04_V_COX_001_STD_0.ACCT_INT_GRUP
-- CREATE VIEW PS_GDW1_BTEQ.D_D04_V_COX_001_STD_0.ACCT_INT_GRUP
-- (
--         ACCT_I,
--         INT_GRUP_I,
--         REL_C,
--         SRCE_SYST_C,
--         VALD_FROM_D,
--         VALD_TO_D,
--         EFFT_D,
--         EXPY_D,
--         PROS_KEY_EFFT_I,
--         PROS_KEY_EXPY_I,
--         ROW_SECU_ACCS_C
-- )
-- AS LOCKING ROW FOR ACCESS

-- SELECT
--         ACCT_I,
--         INT_GRUP_I,
--         REL_C,
--         SRCE_SYST_C,
--         VALD_FROM_D,
--         VALD_TO_D,
--         EFFT_D,
--         EXPY_D,
--         PROS_KEY_EFFT_I,
--         PROS_KEY_EXPY_I,
--         ROW_SECU_ACCS_C
--   FROM
-- P_V_COX_001_STD_0.ACCT_INT_GRUP;

-- PS_GDW1_BTEQ.PVTECH.ACCT_INT_GRUP -> P_V_COX_001_STD_0.ACCT_INT_GRUP
/* <sc-view> P_V_COX_001_STD_0.ACCT_INT_GRUP </sc-view> */

--  8. PVTECH.GRD_PRTF_TYPE_ENHC_HIST_PSST
CREATE OR REPLACE VIEW PS_GDW1_BTEQ.PVTECH.GRD_PRTF_TYPE_ENHC_HIST_PSST AS
SELECT                      
   PRTF_TYPE_C                   
  ,PRTF_TYPE_M                   
  ,PRTF_CLAS_C                   
  ,PRTF_CLAS_M                   
  ,PRTF_CATG_C                   
  ,PRTF_CATG_M                   
  ,VALD_FROM_D
  ,VALD_TO_D
FROM
  PS_CLD_RW.PDGRD.GRD_PRTF_TYPE_ENHC_HIST_PSST
-- $LastChangedBy: jelifft $
-- $LastChangedDate: 2013-10-15 09:26:54 +1100 (Tue, 15 Oct 2013) $
-- $LastChangedRevision: 12741 $
;

--


-- PVTECH.GRD_PRTF_TYPE_ENHC_PSST -> PVTECH.GRD_PRTF_TYPE_ENHC_PSST

/* <sc-view> PS_GDW1_BTEQ.PVTECH.GRD_PRTF_TYPE_ENHC_PSST </sc-view> */

-- --  9. P_P01_PVTECH.DERV_PRTF_ACCT_OWN_REL
-- CREATE  VIEW PS_GDW1_BTEQ.P_P01_PVTECH.DERV_PRTF_ACCT_OWN_REL
-- (
--         ACCT_I,
--   INT_GRUP_I,
--   DERV_PRTF_CATG_C,
--   DERV_PRTF_CLAS_C,
--   DERV_PRTF_TYPE_C,
--   PRTF_ACCT_VALD_FROM_D,
--   PRTF_ACCT_VALD_TO_D,
--   PRTF_ACCT_EFFT_D,
--   PRTF_ACCT_EXPY_D,
--   PRTF_OWN_VALD_FROM_D,
--   PRTF_OWN_VALD_TO_D,
--   PRTF_OWN_EFFT_D,
--   PRTF_OWN_EXPY_D,
--   PTCL_N,
--   REL_MNGE_I,
--   PRTF_CODE_X,
--   DERV_PRTF_ROLE_C,
--   ROLE_PLAY_TYPE_X,
--   ROLE_PLAY_I,
--   SRCE_SYST_C,
--   ROW_SECU_ACCS_C
-- ) AS
-- SELECT
--         ACCT_I,
--   INT_GRUP_I,
--   DERV_PRTF_CATG_C,
--   DERV_PRTF_CLAS_C,
--   DERV_PRTF_TYPE_C,
--   PRTF_ACCT_VALD_FROM_D,
--   PRTF_ACCT_VALD_TO_D,
--   PRTF_ACCT_EFFT_D,
--   PRTF_ACCT_EXPY_D,
--   PRTF_OWN_VALD_FROM_D,
--   PRTF_OWN_VALD_TO_D,
--   PRTF_OWN_EFFT_D,
--   PRTF_OWN_EXPY_D,
--   PTCL_N,
--   REL_MNGE_I,
--   PRTF_CODE_X,
--   DERV_PRTF_ROLE_C,
--   ROLE_PLAY_TYPE_X,
--   ROLE_PLAY_I,
--   SRCE_SYST_C,
--   ROW_SECU_ACCS_C
--   FROM
--         PS_CLD_RW.PP01STARCADPRODDATA.DERV_PRTF_ACCT_OWN_REL

--  WHERE ( 
-- -- if row_secu_accs_c = 0 then the row can be returned
--         ROW_SECU_ACCS_C = 0
--     OR
-- -- the following case returns 1 if the row can be read and 0 if the
-- -- row cannot be read
-- -- the return value from the case is tested against 1 to allow the
-- -- row to be returned
-- -- currently this is restricted to profile values from 0 to 512
-- -- this is because we store the profile as a character
-- -- representation of the number in the three characters starting at
-- -- position 2
-- -- this allows a max value for the profile of 999 and that can be
-- -- accomodated in 10 binary bits which equates to 512 - hence the
-- -- upper limit

-- -- please note that the sequence of tests has been determined to
-- -- give the best response to the widest set of users

-- -- test the return from the case against the value 1
--         1 = CASE

-- -- test  1 - if past 0 then no point in continuing if the profile is
-- --           null
--                 WHEN
--                         PROFILE IS NULL
--                 THEN
--                         0

-- -- test  2 - if past 0 then no point in continuing if the profile = 0
--                 WHEN
--                         CAST (SUBSTRING(PROFILE,2,3) AS INTEGER) = 0
--                 THEN
--                         0

-- -- test  3 - if row_secu_accs_c and profiles are equal then the row
-- --           can be returned
--                 WHEN
--                         ROW_SECU_ACCS_C = CAST (SUBSTRING(PROFILE,2,3) AS INTEGER)
--                 THEN
--                         1

-- -- test  4 - if the first bit is set on both row_secu_accs_c & the
-- --           profile then the row can be returned
--                 WHEN
--                         CAST (SUBSTRING(PROFILE,2,3) AS INTEGER) MOD 2 = 1
--                  AND
--                         ROW_SECU_ACCS_C MOD 2 = 1
--                 THEN
--                         1

-- -- test  5 - if past 1 then no point in continuing if either the
-- --           row_secu_accs_c or the profile < 2
--                 WHEN
--                         CAST (SUBSTRING(PROFILE,2,3) AS INTEGER) < 2
--                   OR
--                         ROW_SECU_ACCS_C < 2
--                 THEN
--                         0

-- -- test  6 - if the second bit is set on both row_secu_accs_c & the
-- --           profile then the row can be returned
--                 WHEN
--                         (CAST (SUBSTRING(PROFILE,2,3) AS INTEGER)/2) MOD 2 = 1
--                  AND
--                         (ROW_SECU_ACCS_C/2) MOD 2 = 1
--                 THEN
--                         1

-- -- test  7 - if past 2 then no point in continuing if either the
-- --           row_secu_accs_c or the profile < 4
--                 WHEN
--                         CAST (SUBSTRING(PROFILE,2,3) AS INTEGER) < 4
--                   OR
--                         ROW_SECU_ACCS_C < 4
--                 THEN
--                         0

-- -- test  8 - if the third bit is set on both row_secu_accs_c & the
-- --           profile then the row can be returned
--                 WHEN
--                         (CAST (SUBSTRING(PROFILE,2,3) AS INTEGER)/4) MOD 2 = 1
--                  AND
--                         (ROW_SECU_ACCS_C/4) MOD 2 = 1
--                 THEN
--                         1

-- -- test  9 - if past 4 then no point in continuing if either the
-- --           row_secu_accs_c or the profile < 8
--                 WHEN
--                         CAST (SUBSTRING(PROFILE,2,3) AS INTEGER) < 8
--                   OR
--                         ROW_SECU_ACCS_C < 8
--                 THEN
--                         0

-- -- test 10 - if the fourth bit is set on both row_secu_accs_c & the
-- --           profile then the row can be returned
--                 WHEN
--                         (CAST (SUBSTRING(PROFILE,2,3) AS INTEGER)/8) MOD 2 = 1
--                  AND
--                         (ROW_SECU_ACCS_C/8) MOD 2 = 1
--                 THEN
--                         1

-- -- test 11 - if past 8 then no point in continuing if either the
-- --           row_secu_accs_c or the profile < 16
--                 WHEN
--                         CAST (SUBSTRING(PROFILE,2,3) AS INTEGER) < 16
--                   OR
--                         ROW_SECU_ACCS_C < 16
--                 THEN
--                         0

-- -- test 12 - if the fifth bit is set on both row_secu_accs_c & the
-- --           profile then the row can be returned
--                 WHEN
--                         (CAST (SUBSTRING(PROFILE,2,3) AS INTEGER)/16) MOD 2 = 1
--                  AND
--                         (ROW_SECU_ACCS_C/16) MOD 2 = 1
--                 THEN
--                         1

-- -- test 13 - if past 16 then no point in continuing if either the
-- --           row_secu_accs_c or the profile < 32
--                 WHEN
--                         CAST (SUBSTRING(PROFILE,2,3) AS INTEGER) < 32
--                   OR
--                         ROW_SECU_ACCS_C < 32
--                 THEN
--                         0

-- -- test 14 - if the sixth bit is set on both row_secu_accs_c & the
-- --           profile then the row can be returned
--                 WHEN
--                         (CAST (SUBSTRING(PROFILE,2,3) AS INTEGER)/32) MOD 2 = 1
--                  AND
--                         (ROW_SECU_ACCS_C/32) MOD 2 = 1
--                 THEN
--                         1

-- -- test 15 - if past 32 then no point in continuing if either the
-- --           row_secu_accs_c or the profile < 64
--                 WHEN
--                         CAST (SUBSTRING(PROFILE,2,3) AS INTEGER) < 64
--                   OR
--                         ROW_SECU_ACCS_C < 64
--                 THEN
--                         0

-- -- test 16 - if the seventh bit is set on both row_secu_accs_c & the
-- --           profile then the row can be returned
--                 WHEN
--                         (CAST (SUBSTRING(PROFILE,2,3) AS INTEGER)/64) MOD 2 = 1
--                  AND
--                         (ROW_SECU_ACCS_C/64) MOD 2 = 1
--                 THEN
--                         1

-- -- test 17 - if past 64 then no point in continuing if either the
-- --           row_secu_accs_c or the profile < 128
--                 WHEN
--                         CAST (SUBSTRING(PROFILE,2,3) AS INTEGER) < 128
--                   OR
--                         ROW_SECU_ACCS_C < 128
--                 THEN
--                         0

-- -- test 18 - if the eighth bit is set on both row_secu_accs_c & the
-- --           profile then the row can be returned
--                 WHEN
--                         (CAST (SUBSTRING(PROFILE,2,3) AS INTEGER)/128) MOD 2 = 1
--                  AND
--                         (ROW_SECU_ACCS_C/128) MOD 2 = 1
--                 THEN
--                         1

-- -- test 19 - if past 128 then no point in continuing if either the
-- --           row_secu_accs_c or the profile < 256
--                 WHEN
--                         CAST (SUBSTRING(PROFILE,2,3) AS INTEGER) < 256
--                   OR
--                         ROW_SECU_ACCS_C < 256
--                 THEN
--                         0

-- -- test 20 - if the ninth bit is set on both row_secu_accs_c & the
-- --           profile then the row can be returned
--                 WHEN
--                         (CAST (SUBSTRING(PROFILE,2,3) AS INTEGER)/256) MOD 2 = 1
--                  AND
--                         (ROW_SECU_ACCS_C/256) MOD 2 = 1
--                 THEN
--                         1

-- -- test 21 - if past 256 then no point in continuing if either the
-- --           row_secu_accs_c or the profile < 512
--                 WHEN
--                         CAST (SUBSTRING(PROFILE,2,3) AS INTEGER) < 512
--                   OR
--                         ROW_SECU_ACCS_C < 512
--                 THEN
--                         0

-- -- test 22 - if the tenth bit is set on both row_secu_accs_c & the
-- --           profile then the row can be returned
--                 WHEN
--                         (CAST (SUBSTRING(PROFILE,2,3) AS INTEGER)/512) MOD 2 = 1
--                  AND
--                         (ROW_SECU_ACCS_C/512) MOD 2 = 1
--                 THEN
--                         1

-- -- otherwise the row cannot be read
--                 ELSE
--                         0

--         END
--     );


-- STAR_CAD_PROD_DATA.DERV_PRTF_ACCT_OWN_REL -> PVTECH.DERV_PRTF_ACCT_OWN_REL

/* <sc-view> PS_GDW1_BTEQ.PVTECH.DERV_PRTF_ACCT_OWN_REL </sc-view> */



 

-- 10. PVTECH.DERV_PRTF_INT_GRUP_OWN_PSST
CREATE OR REPLACE VIEW PS_GDW1_BTEQ.PVTECH.DERV_PRTF_INT_GRUP_OWN_PSST AS
SELECT
   INT_GRUP_I                    
  ,ROLE_PLAY_I                   
  ,EFFT_D                        
  ,EXPY_D                        
  ,VALD_FROM_D                   
  ,VALD_TO_D                     
  ,PERD_D                        
  ,ROLE_PLAY_TYPE_X              
  ,DERV_PRTF_ROLE_C              
  ,SRCE_SYST_C    
  ,ROW_N
  ,ROW_SECU_ACCS_C               
  ,PROS_KEY_I                    
FROM
  PS_CLD_RW.STARCADPRODDATA.DERV_PRTF_INT_GRUP_OWN_PSST
-- $LastChangedBy: jelifft $
-- $LastChangedDate: 2013-07-24 11:07:50 +1000 (Wed, 24 Jul 2013) $
-- $LastChangedRevision: 12357 $
;


-- STAR_CAD_PROD_DATA.DERV_PRTF_INT_HIST_PSST -> PVTECH.DERV_PRTF_INT_HIST_PSST

/* <sc-view> PS_GDW1_BTEQ.PVTECH.DERV_PRTF_INT_HIST_PSST </sc-view> */

-- 11. PVTECH.DERV_PRTF_PATY_OWN_REL (depends on: PVSECURITY.ROW_LEVL_SECU_USER_PRFL)
CREATE OR REPLACE VIEW PS_GDW1_BTEQ.PVTECH.DERV_PRTF_PATY_OWN_REL
(
        PATY_I,
	 INT_GRUP_I,
	 DERV_PRTF_CATG_C,
	 DERV_PRTF_CLAS_C,
	 DERV_PRTF_TYPE_C,
	 PRTF_PATY_VALD_FROM_D,
	 PRTF_PATY_VALD_TO_D,
	 PRTF_PATY_EFFT_D,
	 PRTF_PATY_EXPY_D,
	 PRTF_OWN_VALD_FROM_D,
	 PRTF_OWN_VALD_TO_D,
	 PRTF_OWN_EFFT_D,
	 PRTF_OWN_EXPY_D,
	 PTCL_N,
	 REL_MNGE_I,
	 PRTF_CODE_X,
	 DERV_PRTF_ROLE_C,
	 ROLE_PLAY_TYPE_X,
	 ROLE_PLAY_I,
	 SRCE_SYST_C,
	 ROW_SECU_ACCS_C
) AS
SELECT
        PATY_I,
	 INT_GRUP_I,
	 DERV_PRTF_CATG_C,
	 DERV_PRTF_CLAS_C,
	 DERV_PRTF_TYPE_C,
	 PRTF_PATY_VALD_FROM_D,
	 PRTF_PATY_VALD_TO_D,
	 PRTF_PATY_EFFT_D,
	 PRTF_PATY_EXPY_D,
	 PRTF_OWN_VALD_FROM_D,
	 PRTF_OWN_VALD_TO_D,
	 PRTF_OWN_EFFT_D,
	 PRTF_OWN_EXPY_D,
	 PTCL_N,
	 REL_MNGE_I,
	 PRTF_CODE_X,
	 DERV_PRTF_ROLE_C,
	 ROLE_PLAY_TYPE_X,
	 ROLE_PLAY_I,
	 SRCE_SYST_C,
	 ROW_SECU_ACCS_C
  FROM
        PS_CLD_RW.STARCADPRODDATA.DERV_PRTF_PATY_OWN_REL

 WHERE ( 
(
/* Start - RLS */
COALESCE(ROW_SECU_ACCS_C,0) = 0 OR
GETBIT( (
        SELECT
                    try_to_number(ROW_SECU_PRFL_C::varchar)  --Need to double check this logic
        FROM
                    PS_GDW1_BTEQ.PVSECURITY.ROW_LEVL_SECU_USER_PRFL
        WHERE
                    UPPER(RTRIM( username)) = UPPER(RTRIM(CURRENT_USER))
        ),ROW_SECU_ACCS_C
        ) = 1
/* End - RLS */
)
    );




-- PVTECH.GRD_PRTF_TYPE_ENHC_HIST_PSST -> PVTECH.GRD_PRTF_TYPE_ENHC_HIST_PSST

/* <sc-view> PS_GDW1_BTEQ.PVTECH.GRD_PRTF_TYPE_ENHC_HIST_PSST </sc-view> */
/* Create the new view */


-- 13. PVTECH.DERV_PRTF_INT_GRUP_ENHC_PSST
CREATE OR REPLACE VIEW PS_GDW1_BTEQ.PVTECH.DERV_PRTF_INT_GRUP_ENHC_PSST AS
SELECT
   INT_GRUP_I                    
  ,INT_GRUP_TYPE_C               
  ,EFFT_D                        
  ,EXPY_D                        
  ,VALD_FROM_D                   
  ,VALD_TO_D                     
  ,PERD_D                        
  ,PTCL_N                        
  ,REL_MNGE_I  
  ,ROW_N
  ,PROS_KEY_I                    
FROM
  PS_CLD_RW.STARCADPRODDATA.DERV_PRTF_INT_GRUP_ENHC_PSST
-- $LastChangedBy: jelifft $
-- $LastChangedDate: 2013-07-24 11:07:50 +1000 (Wed, 24 Jul 2013) $
-- $LastChangedRevision: 12357 $
;


-- STAR_CAD_PROD_DATA.DERV_PRTF_INT_GRUP_OWN_PSST -> PVTECH.DERV_PRTF_INT_GRUP_OWN_PSST

/* <sc-view> PS_GDW1_BTEQ.PVTECH.DERV_PRTF_INT_GRUP_OWN_PSST </sc-view> */

-- 14. PVTECH.DERV_PRTF_ACCT_HIST_PSST
CREATE OR REPLACE VIEW PS_GDW1_BTEQ.PVTECH.DERV_PRTF_ACCT_HIST_PSST AS
SELECT
   INT_GRUP_I                    
  ,ACCT_I                        
  ,REL_C                         
  ,JOIN_FROM_D                   
  ,JOIN_TO_D                     
  ,VALD_FROM_D                   
  ,VALD_TO_D                     
  ,EFFT_D                        
  ,EXPY_D                        
  ,SRCE_SYST_C                   
  ,ROW_SECU_ACCS_C               
FROM
  PS_CLD_RW.STARCADPRODDATA.DERV_PRTF_ACCT_HIST_PSST
-- $LastChangedBy: jelifft $
-- $LastChangedDate: 2013-07-24 11:07:50 +1000 (Wed, 24 Jul 2013) $
-- $LastChangedRevision: 12357 $
;


-- STAR_CAD_PROD_DATA.DERV_PRTF_ACCT_INT_GRUP_PSST -> PVTECH.DERV_PRTF_ACCT_INT_GRUP_PSST

/* <sc-view> PS_GDW1_BTEQ.PVTECH.DERV_PRTF_ACCT_INT_GRUP_PSST </sc-view> */

-- 15. PVTECH.DERV_PRTF_OWN_HIST_PSST
CREATE OR REPLACE VIEW PS_GDW1_BTEQ.PVTECH.DERV_PRTF_OWN_HIST_PSST AS
SELECT
   INT_GRUP_I                    
  ,ROLE_PLAY_I                   
  ,JOIN_FROM_D                   
  ,JOIN_TO_D                     
  ,EFFT_D                        
  ,EXPY_D                        
  ,VALD_FROM_D                   
  ,VALD_TO_D                                            
  ,ROLE_PLAY_TYPE_X              
  ,DERV_PRTF_ROLE_C              
  ,SRCE_SYST_C                   
  ,ROW_SECU_ACCS_C               
  ,PROS_KEY_I                                      
FROM
  PS_CLD_RW.STARCADPRODDATA.DERV_PRTF_OWN_HIST_PSST
-- $LastChangedBy: jelifft $
-- $LastChangedDate: 2013-07-22 10:21:37 +1000 (Mon, 22 Jul 2013) $
-- $LastChangedRevision: 12328 $
;


-- STAR_CAD_PROD_DATA.DERV_PRTF_OWN_PSST -> PVTECH.DERV_PRTF_OWN_PSST

/* <sc-view> PS_GDW1_BTEQ.PVTECH.DERV_PRTF_OWN_PSST </sc-view> */
/* Re-place the existing views */


-- -- 16. JELIFFT.GRD_PRTF_TYPE_ENHC (depends on: PVTECH.CALENDAR, PVTECH.DIMN_NODE_ASSC, PVTECH.GRD_PRTF_CATG_ATTR, PVTECH.GRD_PRTF_CLAS_ATTR, PVTECH.GRD_PRTF_TYPE_ATTR, PVTECH.MAP_SAP_INT_GRUP)
-- CREATE  VIEW PS_GDW1_BTEQ.JELIFFT.GRD_PRTF_TYPE_ENHC as
-- Select
--    GPTA6.PERD_D                 as PERD_D
--   ,GPTA6.PRTF_TYPE_C            as PRTF_TYPE_C
--   ,GPTA6.PRTF_TYPE_M            as PRTF_TYPE_M
--   ,GPCL6.PRTF_CLAS_C            as PRTF_CLAS_C
--   ,GPCL6.PRTF_CLAS_M            as PRTF_CLAS_M
--   ,GPCA6.PRTF_CATG_C            as PRTF_CATG_C
--   ,GPCA6.PRTF_CATG_M            as PRTF_CATG_M
-- From  
--   (
--     Select
--        G.PRTF_TYPE_C
--       ,G.PRTF_TYPE_M
--       ,G.CLAS_SCHM_C
--       ,G.PRTF_TYPE_NODE_C
--       ,G.EFFT_D
--       ,C.CALENDAR_DATE as PERD_D
--     From
--       PS_GDW1_BTEQ.PVTECH.GRD_PRTF_TYPE_ATTR G
--       Inner Join PS_GDW1_BTEQ.PVTECH.MAP_SAP_INT_GRUP MSIG
--       On MSIG.BUSN_PTNR_GRUP_TYPE = G.SAP_C
--       And G.CLAS_SCHM_C = 'PRTF_TYPE'

--       Inner Join PS_GDW1_BTEQ.PVTECH.CALENDAR C
--       On C.CALENDAR_DATE between G.NODE_EFFT_D and G.NODE_EXPY_D
--       And C.CALENDAR_DATE between MSIG.EFFT_D and MSIG.EXPY_D
--       And C.CALENDAR_DATE between DATEADD(MONTH, -39, (CURRENT_DATE - EXTRACT(DAY FROM CURRENT_DATE) +1 ) ) and DATEADD(MONTH, 1, CURRENT_DATE)
--       And G.EXPY_D = '9999-12-31'

--     Where
--       G.CLAS_SCHM_C = 'PRTF_TYPE'
--     Qualify Rank() Over (Partition By G.PRTF_TYPE_NODE_C, C.CALENDAR_DATE
--                           Order By G.EFFT_D Desc ) = 1
--   ) as GPTA6

--   Inner Join (
--     Select
--        D61.CLAS_SCHM_1_C
--       ,D61.CLAS_SCHM_2_C
--       ,D61.DIMN_NODE_1_C
--       ,D61.DIMN_NODE_2_C
--       ,D61.EFFT_D
--       ,C.CALENDAR_DATE as PERD_D
--     From
--       PS_GDW1_BTEQ.PVTECH.DIMN_NODE_ASSC D61 
--       Inner Join PS_GDW1_BTEQ.PVTECH.CALENDAR C
--       On C.CALENDAR_DATE between D61.BUSN_EFFT_D AND D61.BUSN_EXPY_D
--       And D61.EXPY_D = '9999-12-31'
--       And C.CALENDAR_DATE between DATEADD(MONTH, -39, (CURRENT_DATE - EXTRACT(DAY FROM CURRENT_DATE) +1 ) ) and DATEADD(MONTH, 1, CURRENT_DATE)
--     Where
--         D61.CLAS_SCHM_1_C = 'PRTF_CLAS'
--         And D61.DIMN_NODE_ASSC_TYPE_C = 'PRTF_TYPE_CLAS' 
--     Qualify Rank() Over (Partition By D61.DIMN_NODE_1_C, D61.DIMN_NODE_2_C, C.CALENDAR_DATE
--                           Order By D61.EFFT_D Desc ) = 1

--   ) DNA61
  
--   On DNA61.CLAS_SCHM_2_C = GPTA6.CLAS_SCHM_C
--   And DNA61.DIMN_NODE_2_C = GPTA6.PRTF_TYPE_NODE_C
--   And GPTA6.PERD_D = DNA61.PERD_D

--   Inner Join (
--     Select
--        G.PRTF_CLAS_C
--       ,G.PRTF_CLAS_M  
--       ,G.CLAS_SCHM_C
--       ,G.PRTF_CLAS_NODE_C
--       ,G.EFFT_D
--       ,C.CALENDAR_DATE as PERD_D
--     From
--       PS_GDW1_BTEQ.PVTECH.GRD_PRTF_CLAS_ATTR G
--       Inner Join PS_GDW1_BTEQ.PVTECH.CALENDAR C
--       On C.CALENDAR_DATE between G.NODE_EFFT_D AND G.NODE_EXPY_D 
--       And G.EXPY_D = '9999-12-31'
--       And C.CALENDAR_DATE between DATEADD(MONTH, -39, (CURRENT_DATE - EXTRACT(DAY FROM CURRENT_DATE) +1 ) ) and DATEADD(MONTH, 1, CURRENT_DATE)

--     Qualify Rank() Over (Partition By G.PRTF_CLAS_NODE_C, C.CALENDAR_DATE
--                           Order By G.EFFT_D Desc ) = 1

--   ) as GPCL6
--   On GPCL6.CLAS_SCHM_C = DNA61.CLAS_SCHM_1_C
--   And GPCL6.PRTF_CLAS_NODE_C = DNA61.DIMN_NODE_1_C
--   And GPCL6.PERD_D = DNA61.PERD_D

--   Inner Join (  
--     Select
--        D62.CLAS_SCHM_1_C
--       ,D62.CLAS_SCHM_2_C
--       ,D62.DIMN_NODE_1_C
--       ,D62.DIMN_NODE_2_C
--       ,D62.EFFT_D
--       ,C.CALENDAR_DATE as PERD_D
--     From
--       PS_GDW1_BTEQ.PVTECH.DIMN_NODE_ASSC D62 
--       Inner Join PS_GDW1_BTEQ.PVTECH.CALENDAR C
--       On C.CALENDAR_DATE between D62.BUSN_EFFT_D AND D62.BUSN_EXPY_D
--       And D62.EXPY_D = '9999-12-31'
--       And C.CALENDAR_DATE between DATEADD(MONTH, -39, (CURRENT_DATE - EXTRACT(DAY FROM CURRENT_DATE) +1 ) ) and DATEADD(MONTH, 1, CURRENT_DATE)
--     Where
--         D62.CLAS_SCHM_2_C = 'PRTF_CLAS'
--         And D62.CLAS_SCHM_1_C = 'PRTF_CATG'
--         And D62.DIMN_NODE_ASSC_TYPE_C = 'PRTF_CATG_CLAS' 

--     Qualify Rank() Over (Partition By D62.DIMN_NODE_1_C, D62.DIMN_NODE_2_C, C.CALENDAR_DATE
--                           Order By D62.EFFT_D Desc ) = 1

--   ) as DNA62   

--   On DNA62.CLAS_SCHM_2_C = DNA61.CLAS_SCHM_1_C
--   And DNA62.DIMN_NODE_2_C = DNA61.DIMN_NODE_1_C
--   And DNA62.PERD_D = DNA61.PERD_D

--   Inner Join (
--     Select
--        G.PRTF_CATG_C
--       ,G.PRTF_CATG_M
--       ,G.CLAS_SCHM_C
--       ,G.PRTF_CATG_NODE_C
--       ,G.EFFT_D
--       ,C.CALENDAR_DATE as PERD_D
--     From
--       PS_GDW1_BTEQ.PVTECH.GRD_PRTF_CATG_ATTR G
--       Inner Join PS_GDW1_BTEQ.PVTECH.CALENDAR C
--       On C.CALENDAR_DATE between G.NODE_EFFT_D AND G.NODE_EXPY_D 
--       And G.EXPY_D = '9999-12-31'

--       And C.CALENDAR_DATE between DATEADD(MONTH, -39, (CURRENT_DATE - EXTRACT(DAY FROM CURRENT_DATE) +1 ) ) and DATEADD(MONTH, 1, CURRENT_DATE)  

--    Qualify Rank() Over (Partition By G.PRTF_CATG_NODE_C, C.CALENDAR_DATE
--                           Order By G.EFFT_D Desc ) = 1
  
--   ) as GPCA6

--   On GPCA6.CLAS_SCHM_C =  DNA62.CLAS_SCHM_1_C
--   And GPCA6.PRTF_CATG_NODE_C = DNA62.DIMN_NODE_1_C
--   And GPCA6.PERD_D = DNA62.PERD_D

-- Group By 1,2,3,4,5,6,7

-- $LastChangedBy: jelifft $
-- $LastChangedDate: 2013-07-17 12:11:03 +1000 (Wed, 17 Jul 2013) $
-- $LastChangedRevision: 12304 $
;


-- PVTECH.GRD_PRTF_TYPE_ENHC -> PVTECH.GRD_PRTF_TYPE_ENHC

/* <sc-view> PS_GDW1_BTEQ.PVTECH.GRD_PRTF_TYPE_ENHC </sc-view> */

--
--  SCRIPT NAME: 60_CRAT_VIEW_GRD_PRTF_TYPE_ENHC.SQL
--
-- Ver Date       Modified By Description
-- --- ---------- 
--                          
-- 1.0  12/04/2013 T Jelliffe Initial Version
-- 1.1  24/04/2013 T Jelliffe S2T v1.7
-- 1.2  29/04/2013 T Jelliffe Rename GRD_PRTF_ATTR as GRD_PRTF_TYPE_ENHC
-- 1.3  01/05/2013 T Jelliffe Add EFFT_D/EXPY_D Filter
-- 1.4  03/05/2013 T Jelliffe Add CALENDER_DATE to Partition criteria
-- 1.5  14/05/2013 T Jelliffe Remove SHDW_RPRT_F
-- 1.6  29/05/2013 T Jelliffe S2T v1.12
-- 1.7  04/06/2013 T Jelliffe Remove anchor on EFFT_D/EXPY_D
-- 1.8  11/06/2013 T Jelliffe Remove anchor on BUSN dates, EFFT_D instead
-- 1.9  20/06/2013 T Jelliffe Anchor on NODE dates
-- 1.10 17/07/2013 T Jelliffe 39 months history range
-- 1.11 18/11/2013 T Jelliffe Add MAP_SAP_INT_GRUP table

-- This info is for CBM use only    



-- 17. PVTECH.DERV_PRTF_ACCT_PSST
CREATE OR REPLACE VIEW PS_GDW1_BTEQ.PVTECH.DERV_PRTF_ACCT_PSST AS
SELECT
   INT_GRUP_I                    
  ,ACCT_I                        
  ,JOIN_FROM_D                   
  ,JOIN_TO_D                     
  ,REL_C                         
  ,VALD_FROM_D                   
  ,VALD_TO_D                     
  ,EFFT_D                        
  ,EXPY_D                        
  ,SRCE_SYST_C  
  ,PROS_KEY_I
  ,ROW_SECU_ACCS_C                                                        
FROM
  PS_CLD_RW.STARCADPRODDATA.DERV_PRTF_ACCT_PSST
-- $LastChangedBy: jelifft $
-- $LastChangedDate: 2013-10-22 16:53:07 +1100 (Tue, 22 Oct 2013) $
-- $LastChangedRevision: 12844 $
;


-- STAR_CAD_PROD_DATA.DERV_PRTF_INT_GRUP_ENHC_PSST -> PVTECH.DERV_PRTF_INT_GRUP_ENHC_PSST

/* <sc-view> PS_GDW1_BTEQ.PVTECH.DERV_PRTF_INT_GRUP_ENHC_PSST </sc-view> */

-- 18. PVTECH.GRD_PRTF_TYPE_ENHC_PSST
CREATE OR REPLACE VIEW PS_GDW1_BTEQ.PVTECH.GRD_PRTF_TYPE_ENHC_PSST AS
SELECT
   PERD_D                        
  ,PRTF_TYPE_C                   
  ,PRTF_TYPE_M                   
  ,PRTF_CLAS_C                   
  ,PRTF_CLAS_M                   
  ,PRTF_CATG_C                   
  ,PRTF_CATG_M                   
FROM
  PS_CLD_RW.PDGRD.GRD_PRTF_TYPE_ENHC_PSST
-- $LastChangedBy: jelifft $
-- $LastChangedDate: 2013-07-24 11:07:50 +1000 (Wed, 24 Jul 2013) $
-- $LastChangedRevision: 12357 $
;


-- STAR_CAD_PROD_DATA.ACCT_BALN_BKDT -> PVTECH.ACCT_BALN_BKDT

/* <sc-view> PS_GDW1_BTEQ.PVTECH.ACCT_BALN_BKDT </sc-view> */



 

-- 19. P_P01_PVTECH.ACCT_PATY_TAX_INSS (depends on: P_P01_PVSECURITY.ROW_LEVL_SECU_USER_PRFL)
-- CREATE  VIEW PS_GDW1_BTEQ.P_P01_PVTECH.ACCT_PATY_TAX_INSS
-- (
--         ACCT_I,
--         PATY_I,
--         SRCE_SYST_C,
--         RESI_STUS_C,
--         IDNN_TYPE_C,
--         IDNN_STUS_C,
--         EFFT_D,
--         EXPY_D,
--         PROS_KEY_EFFT_I,
--         PROS_KEY_EXPY_I,
--         ROW_SECU_ACCS_C
-- ) AS
-- SELECT
--         ACCT_I,
--         PATY_I,
--         SRCE_SYST_C,
--         RESI_STUS_C,
--         IDNN_TYPE_C,
--         IDNN_STUS_C,
--         EFFT_D,
--         EXPY_D,
--         PROS_KEY_EFFT_I,
--         PROS_KEY_EXPY_I,
--         ROW_SECU_ACCS_C
--   FROM (
-- SELECT
--         ACCT_I,
--         PATY_I,
--         SRCE_SYST_C,
--         RESI_STUS_C,
--         IDNN_TYPE_C,
--         IDNN_STUS_C,
--         EFFT_D,
--         EXPY_D,
--         PROS_KEY_EFFT_I,
--         PROS_KEY_EXPY_I,
--         CASE WHEN ACCT_I LIKE 'FMS%' THEN 1 ELSE ROW_SECU_ACCS_C end AS ROW_SECU_ACCS_C
--   FROM
--         PS_CLD_RW.PP01STARCADPRODDATA.ACCT_PATY_TAX_INSS ) ACCT_PATY_TAX_INSS
--  WHERE
-- (
-- /* Start - RLS */
-- COALESCE(ROW_SECU_ACCS_C,0) = 0 OR
-- GETBIT( (
--         SELECT
--                     try_to_number(ROW_SECU_PRFL_C::varchar)  --Need to double check this logic
--         FROM
--                     PS_GDW1_BTEQ.P_P01_PVSECURITY.ROW_LEVL_SECU_USER_PRFL
--         WHERE
--                     UPPER(RTRIM( username)) = UPPER(RTRIM(CURRENT_USER))
--         ),ROW_SECU_ACCS_C
--         ) = 1
-- /* End - RLS */
-- );

-- PS_CLD_RW.STARCADPRODDATA.ACCT_PATY_TAX_INSS -> P_V_COX_001_STD_0.ACCT_PATY_TAX_INSS
/* <sc-view> P_V_COX_001_STD_0.ACCT_PATY_TAX_INSS </sc-view> */

-- 20. PVTECH.ACCT_PATY_TAX_INSS (depends on: PVSECURITY.ROW_LEVL_SECU_USER_PRFL)
-- CREATE  VIEW PS_GDW1_BTEQ.PVTECH.ACCT_PATY_TAX_INSS
-- (
--         ACCT_I,
--         PATY_I,
--         SRCE_SYST_C,
--         RESI_STUS_C,
--         IDNN_TYPE_C,
--         IDNN_STUS_C,
--         EFFT_D,
--         EXPY_D,
--         PROS_KEY_EFFT_I,
--         PROS_KEY_EXPY_I,
--         ROW_SECU_ACCS_C
-- ) AS
-- SELECT
--         ACCT_I,
--         PATY_I,
--         SRCE_SYST_C,
--         RESI_STUS_C,
--         IDNN_TYPE_C,
--         IDNN_STUS_C,
--         EFFT_D,
--         EXPY_D,
--         PROS_KEY_EFFT_I,
--         PROS_KEY_EXPY_I,
--         ROW_SECU_ACCS_C
--   FROM (
-- SELECT
--         ACCT_I,
--         PATY_I,
--         SRCE_SYST_C,
--         RESI_STUS_C,
--         IDNN_TYPE_C,
--         IDNN_STUS_C,
--         EFFT_D,
--         EXPY_D,
--         PROS_KEY_EFFT_I,
--         PROS_KEY_EXPY_I,
--         CASE WHEN ACCT_I LIKE 'FMS%' THEN 1 ELSE ROW_SECU_ACCS_C end AS ROW_SECU_ACCS_C
--   FROM
--         PS_CLD_RW.STARCADPRODDATA.ACCT_PATY_TAX_INSS ) ACCT_PATY_TAX_INSS
--  WHERE
-- (
-- /* Start - RLS */
-- COALESCE(ROW_SECU_ACCS_C,0) = 0 OR
-- GETBIT( (
--         SELECT
--                     try_to_number(ROW_SECU_PRFL_C::varchar)  --Need to double check this logic
--         FROM
--                     PS_GDW1_BTEQ.PVSECURITY.ROW_LEVL_SECU_USER_PRFL
--         WHERE
--                     UPPER(RTRIM( username)) = UPPER(RTRIM(CURRENT_USER))
--         ),ROW_SECU_ACCS_C
--         ) = 1
-- /* End - RLS */
-- );


-- STAR_CAD_PROD_DATA.DERV_PRTF_ACCT_HIST_PSST -> PVTECH.DERV_PRTF_ACCT_HIST_PSST

/* <sc-view> PS_GDW1_BTEQ.PVTECH.DERV_PRTF_ACCT_HIST_PSST </sc-view> */

-- 21. PVTECH.DERV_PRTF_INT_PSST
CREATE OR REPLACE VIEW PS_GDW1_BTEQ.PVTECH.DERV_PRTF_INT_PSST AS
SELECT
   INT_GRUP_I                    
  ,INT_GRUP_TYPE_C               
  ,JOIN_FROM_D                   
  ,JOIN_TO_D                     
  ,EFFT_D                        
  ,EXPY_D                        
  ,PTCL_N                        
  ,REL_MNGE_I                    
  ,VALD_FROM_D                   
  ,VALD_TO_D                     
  ,PROS_KEY_I                      
FROM
  PS_CLD_RW.STARCADPRODDATA.DERV_PRTF_INT_PSST
-- $LastChangedBy: jelifft $
-- $LastChangedDate: 2013-10-21 16:39:07 +1100 (Mon, 21 Oct 2013) $
-- $LastChangedRevision: 12832 $
;


-- STAR_CAD_PROD_DATA.DERV_PRTF_OWN_HIST_PSST -> PVTECH.DERV_PRTF_OWN_HIST_PSST

/* <sc-view> PS_GDW1_BTEQ.PVTECH.DERV_PRTF_OWN_HIST_PSST </sc-view> */
/* Re-place the existing views */


-- 22. PVTECH.DERV_PRTF_ACCT_OWN_REL (depends on: PVSECURITY.ROW_LEVL_SECU_USER_PRFL)
CREATE OR REPLACE VIEW PS_GDW1_BTEQ.PVTECH.DERV_PRTF_ACCT_OWN_REL
(
        ACCT_I,
	 INT_GRUP_I,
	 DERV_PRTF_CATG_C,
	 DERV_PRTF_CLAS_C,
	 DERV_PRTF_TYPE_C,
	 PRTF_ACCT_VALD_FROM_D,
	 PRTF_ACCT_VALD_TO_D,
	 PRTF_ACCT_EFFT_D,
	 PRTF_ACCT_EXPY_D,
	 PRTF_OWN_VALD_FROM_D,
	 PRTF_OWN_VALD_TO_D,
	 PRTF_OWN_EFFT_D,
	 PRTF_OWN_EXPY_D,
	 PTCL_N,
	 REL_MNGE_I,
	 PRTF_CODE_X,
	 DERV_PRTF_ROLE_C,
	 ROLE_PLAY_TYPE_X,
	 ROLE_PLAY_I,
	 SRCE_SYST_C,
	 ROW_SECU_ACCS_C
) AS
SELECT
        ACCT_I,
	 INT_GRUP_I,
	 DERV_PRTF_CATG_C,
	 DERV_PRTF_CLAS_C,
	 DERV_PRTF_TYPE_C,
	 PRTF_ACCT_VALD_FROM_D,
	 PRTF_ACCT_VALD_TO_D,
	 PRTF_ACCT_EFFT_D,
	 PRTF_ACCT_EXPY_D,
	 PRTF_OWN_VALD_FROM_D,
	 PRTF_OWN_VALD_TO_D,
	 PRTF_OWN_EFFT_D,
	 PRTF_OWN_EXPY_D,
	 PTCL_N,
	 REL_MNGE_I,
	 PRTF_CODE_X,
	 DERV_PRTF_ROLE_C,
	 ROLE_PLAY_TYPE_X,
	 ROLE_PLAY_I,
	 SRCE_SYST_C,
	 ROW_SECU_ACCS_C
  FROM
        PS_CLD_RW.STARCADPRODDATA.DERV_PRTF_ACCT_OWN_REL

 WHERE ( 
(
/* Start - RLS */
COALESCE(ROW_SECU_ACCS_C,0) = 0 OR
GETBIT( (
        SELECT
                    try_to_number(ROW_SECU_PRFL_C::varchar)  --Need to double check this logic
        FROM
                    PS_GDW1_BTEQ.PVSECURITY.ROW_LEVL_SECU_USER_PRFL
        WHERE
                    UPPER(RTRIM( username)) = UPPER(RTRIM(CURRENT_USER))
        ),ROW_SECU_ACCS_C
        ) = 1
/* End - RLS */
)
    );


-- STAR_CAD_PROD_DATA.DERV_PRTF_ACCT_PSST -> PVTECH.DERV_PRTF_ACCT_PSST

/* <sc-view> PS_GDW1_BTEQ.PVTECH.DERV_PRTF_ACCT_PSST </sc-view> */
/* Re-place the existing views */


-- 23. PVTECH.DERV_PRTF_OWN_PSST
CREATE OR REPLACE VIEW PS_GDW1_BTEQ.PVTECH.DERV_PRTF_OWN_PSST AS
SELECT
   INT_GRUP_I   
  ,JOIN_FROM_D                   
  ,JOIN_TO_D                        
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
FROM
  PS_CLD_RW.STARCADPRODDATA.DERV_PRTF_OWN_PSST
-- $LastChangedBy: jelifft $
-- $LastChangedDate: 2013-10-23 09:10:24 +1100 (Wed, 23 Oct 2013) $
-- $LastChangedRevision: 12847 $
;


-- STAR_CAD_PROD_DATA.DERV_PRTF_PATY_HIST_PSST -> PVTECH.DERV_PRTF_PATY_HIST_PSST

/* <sc-view> PS_GDW1_BTEQ.PVTECH.DERV_PRTF_PATY_HIST_PSST </sc-view> */
/* Re-place the existing views */


-- 24. PVTECH.DERV_PRTF_ACCT_INT_GRUP_PSST
CREATE OR REPLACE VIEW PS_GDW1_BTEQ.PVTECH.DERV_PRTF_ACCT_INT_GRUP_PSST AS
SELECT
   INT_GRUP_I                    
  ,ACCT_I                        
  ,EFFT_D                        
  ,EXPY_D                        
  ,VALD_FROM_D                   
  ,VALD_TO_D                     
  ,PERD_D                        
  ,REL_C                         
  ,SRCE_SYST_C  
  ,ROW_N
  ,ROW_SECU_ACCS_C               
  ,PROS_KEY_I                    
FROM
  PS_CLD_RW.STARCADPRODDATA.DERV_PRTF_ACCT_INT_GRUP_PSST
-- $LastChangedBy: jelifft $
-- $LastChangedDate: 2013-07-24 11:07:50 +1000 (Wed, 24 Jul 2013) $
-- $LastChangedRevision: 12357 $
;

-- PS_CLD_RW.STARCADPRODDATA.DERV_PRTF_ACCT_OWN_REL -> PS_GDW1_BTEQ.P_P01_PVTECH.DERV_PRTF_ACCT_OWN_REL
/* <sc-view> PS_GDW1_BTEQ.P_P01_PVTECH.DERV_PRTF_ACCT_OWN_REL </sc-view> */







-- 25. PVTECH.DERV_PRTF_PATY_HIST_PSST
CREATE OR REPLACE VIEW PS_GDW1_BTEQ.PVTECH.DERV_PRTF_PATY_HIST_PSST AS
SELECT
   INT_GRUP_I                    
  ,PATY_I                        
  ,REL_C                         
  ,JOIN_FROM_D                   
  ,JOIN_TO_D                     
  ,VALD_FROM_D                   
  ,VALD_TO_D                     
  ,EFFT_D                        
  ,EXPY_D                        
  ,SRCE_SYST_C                   
  ,ROW_SECU_ACCS_C   
  ,PROS_KEY_I
FROM
  PS_CLD_RW.STARCADPRODDATA.DERV_PRTF_PATY_HIST_PSST
-- $LastChangedBy: jelifft $
-- $LastChangedDate: 2013-07-22 10:21:37 +1000 (Mon, 22 Jul 2013) $
-- $LastChangedRevision: 12328 $
;


-- STAR_CAD_PROD_DATA.DERV_PRTF_PATY_INT_GRUP_PSST -> PVTECH.DERV_PRTF_PATY_INT_GRUP_PSST

/* <sc-view> PS_GDW1_BTEQ.PVTECH.DERV_PRTF_PATY_INT_GRUP_PSST </sc-view> */

-- 26. PVDATA.ACCT_BALN_BKDT_AUDT (depends on: PVTECH.ACCT_BALN_BKDT_AUDT)
CREATE OR REPLACE VIEW PS_GDW1_BTEQ.PVDATA.ACCT_BALN_BKDT_AUDT
(
 ACCT_I,
BALN_TYPE_C,
CALC_FUNC_C,
TIME_PERD_C,
BALN_A,
CALC_F,
SRCE_SYST_C,
ORIG_SRCE_SYST_C,
LOAD_D,
BKDT_EFFT_D,
BKDT_EXPY_D,
PROS_KEY_EFFT_I,
ABAL_PROS_KEY_EFFT_I,
ABAL_PROS_KEY_EXPY_I,
ABAL_BKDT_PROS_KEY_I,
ADJ_PROS_KEY_EFFT_I
)
AS 
SELECT
 ACCT_I,
BALN_TYPE_C,
CALC_FUNC_C,
TIME_PERD_C,
BALN_A,
CALC_F,
SRCE_SYST_C,
ORIG_SRCE_SYST_C,
LOAD_D,
BKDT_EFFT_D,
BKDT_EXPY_D,
PROS_KEY_EFFT_I,
ABAL_PROS_KEY_EFFT_I,
ABAL_PROS_KEY_EXPY_I,
ABAL_BKDT_PROS_KEY_I,
ADJ_PROS_KEY_EFFT_I
FROM
PS_GDW1_BTEQ.PVTECH.ACCT_BALN_BKDT_AUDT;


-- STAR_CAD_PROD_DATA.ACCT_BALN_BKDT_AUDT -> PVTECH.ACCT_BALN_BKDT_AUDT

/* <sc-view> PS_GDW1_BTEQ.PVTECH.ACCT_BALN_BKDT_AUDT </sc-view> */

-- 27. P_V_COX_001_STD_0.ACCT_INT_GRUP (depends on: PVTECH.ACCT_INT_GRUP)
-- CREATE VIEW PS_GDW1_BTEQ.P_V_COX_001_STD_0.ACCT_INT_GRUP
-- (
--         ACCT_I,
--         INT_GRUP_I,
--         REL_C,
--         SRCE_SYST_C,
--         VALD_FROM_D,
--         VALD_TO_D,
--         EFFT_D,
--         EXPY_D,
--         PROS_KEY_EFFT_I,
--         PROS_KEY_EXPY_I,
--         ROW_SECU_ACCS_C
-- )
-- AS LOCKING ROW FOR ACCESS

-- SELECT
--         ACCT_I,
--         INT_GRUP_I,
--         REL_C,
--         SRCE_SYST_C,
--         VALD_FROM_D,
--         VALD_TO_D,
--         EFFT_D,
--         EXPY_D,
--         PROS_KEY_EFFT_I,
--         PROS_KEY_EXPY_I,
--         ROW_SECU_ACCS_C
--   FROM
-- PS_GDW1_BTEQ.PVTECH.ACCT_INT_GRUP
-- ;

-- PS_GDW1_BTEQ.PVTECH.ACCT_INT_GRUP -> P_P01_V_COX_001_STD_0.ACCT_INT_GRUP
/* <sc-view> P_P01_V_COX_001_STD_0.ACCT_INT_GRUP </sc-view> */







-- 28. P_P01_V_COX_001_STD_0.ACCT_INT_GRUP (depends on: P_P01_PVTECH.ACCT_INT_GRUP)
-- CREATE VIEW PS_GDW1_BTEQ.P_P01_V_COX_001_STD_0.ACCT_INT_GRUP
-- (
--         ACCT_I,
--         INT_GRUP_I,
--         REL_C,
--         SRCE_SYST_C,
--         VALD_FROM_D,
--         VALD_TO_D,
--         EFFT_D,
--         EXPY_D,
--         PROS_KEY_EFFT_I,
--         PROS_KEY_EXPY_I,
--         ROW_SECU_ACCS_C
-- )
-- AS LOCKING ROW FOR ACCESS

-- SELECT
--         ACCT_I,
--         INT_GRUP_I,
--         REL_C,
--         SRCE_SYST_C,
--         VALD_FROM_D,
--         VALD_TO_D,
--         EFFT_D,
--         EXPY_D,
--         PROS_KEY_EFFT_I,
--         PROS_KEY_EXPY_I,
--         ROW_SECU_ACCS_C
--   FROM
-- PS_GDW1_BTEQ.P_P01_PVTECH.ACCT_INT_GRUP;

-- PS_GDW1_BTEQ.PVTECH.ACCT_INT_GRUP -> PS_GDW1_BTEQ.P_P01_PVTECH.ACCT_INT_GRUP
/* <sc-view> PS_GDW1_BTEQ.P_P01_PVTECH.ACCT_INT_GRUP </sc-view> */




-- 29. P_P01_V_COX_001_STD_0.ACCT_PATY_TAX_INSS (depends on: P_P01_PVTECH.ACCT_PATY_TAX_INSS)
-- CREATE VIEW PS_GDW1_BTEQ.P_P01_V_COX_001_STD_0.ACCT_PATY_TAX_INSS
-- (
--         ACCT_I,
--         PATY_I,
--         SRCE_SYST_C,
--         RESI_STUS_C,
--         IDNN_TYPE_C,
--         IDNN_STUS_C,
--         EFFT_D,
--         EXPY_D,
--         PROS_KEY_EFFT_I,
--         PROS_KEY_EXPY_I,
--         ROW_SECU_ACCS_C
-- )
--     AS LOCKING ROW FOR ACCESS
-- SELECT
--         ACCT_I,
--         PATY_I,
--         SRCE_SYST_C,
--         RESI_STUS_C,
--         IDNN_TYPE_C,
--         IDNN_STUS_C,
--         EFFT_D,
--         EXPY_D,
--         PROS_KEY_EFFT_I,
--         PROS_KEY_EXPY_I,
--         ROW_SECU_ACCS_C
--   FROM
--         PS_GDW1_BTEQ.P_P01_PVTECH.ACCT_PATY_TAX_INSS;


-- STAR_CAD_PROD_DATA.ACCT_PATY_TAX_INSS -> PVTECH.ACCT_PATY_TAX_INSS

/* <sc-view> PS_GDW1_BTEQ.PVTECH.ACCT_PATY_TAX_INSS </sc-view> */



-- 30. P_V_COX_001_STD_0.ACCT_PATY_TAX_INSS (depends on: PVTECH.ACCT_PATY_TAX_INSS)
-- CREATE VIEW PS_GDW1_BTEQ.P_V_COX_001_STD_0.ACCT_PATY_TAX_INSS
-- (
--         ACCT_I,
--         PATY_I,
--         SRCE_SYST_C,
--         RESI_STUS_C,
--         IDNN_TYPE_C,
--         IDNN_STUS_C,
--         EFFT_D,
--         EXPY_D,
--         PROS_KEY_EFFT_I,
--         PROS_KEY_EXPY_I,
--         ROW_SECU_ACCS_C
-- )
--     AS LOCKING ROW FOR ACCESS
-- SELECT
--         ACCT_I,
--         PATY_I,
--         SRCE_SYST_C,
--         RESI_STUS_C,
--         IDNN_TYPE_C,
--         IDNN_STUS_C,
--         EFFT_D,
--         EXPY_D,
--         PROS_KEY_EFFT_I,
--         PROS_KEY_EXPY_I,
--         ROW_SECU_ACCS_C
--   FROM
--         PS_GDW1_BTEQ.PVTECH.ACCT_PATY_TAX_INSS;

-- PS_CLD_RW.STARCADPRODDATA.ACCT_PATY_TAX_INSS -> D_D04_V_COX_001_STD_0.ACCT_PATY_TAX_INSS
/* <sc-view> D_D04_V_COX_001_STD_0.ACCT_PATY_TAX_INSS </sc-view> */


-- 31. D_D04_V_COX_001_STD_0.ACCT_PATY_TAX_INSS (depends on: PVTECH.ACCT_PATY_TAX_INSS)
-- CREATE VIEW PS_GDW1_BTEQ.D_D04_V_COX_001_STD_0.ACCT_PATY_TAX_INSS
-- (
--         ACCT_I,
--         PATY_I,
--         SRCE_SYST_C,
--         RESI_STUS_C,
--         IDNN_TYPE_C,
--         IDNN_STUS_C,
--         EFFT_D,
--         EXPY_D,
--         PROS_KEY_EFFT_I,
--         PROS_KEY_EXPY_I,
--         ROW_SECU_ACCS_C
-- ) AS
-- SELECT
--         ACCT_I,
--         PATY_I,
--         SRCE_SYST_C,
--         RESI_STUS_C,
--         IDNN_TYPE_C,
--         IDNN_STUS_C,
--         EFFT_D,
--         EXPY_D,
--         PROS_KEY_EFFT_I,
--         PROS_KEY_EXPY_I,
--         ROW_SECU_ACCS_C
--   FROM
--         PS_GDW1_BTEQ.PVTECH.ACCT_PATY_TAX_INSS;

-- PS_CLD_RW.STARCADPRODDATA.ACCT_PATY_TAX_INSS -> P_P01_V_COX_001_STD_0.ACCT_PATY_TAX_INSS
/* <sc-view> P_P01_V_COX_001_STD_0.ACCT_PATY_TAX_INSS </sc-view> */







-- 12. PVTECH.GRD_PRTF_TYPE_ENHC (depends on: PVTECH.CALENDAR, PVTECH.DIMN_NODE_ASSC, PVTECH.GRD_PRTF_CATG_ATTR, PVTECH.GRD_PRTF_CLAS_ATTR, PVTECH.GRD_PRTF_TYPE_ATTR, PVTECH.MAP_SAP_INT_GRUP, PVTECH.TYPE_INT_GRUP)
CREATE OR REPLACE VIEW PS_GDW1_BTEQ.PVTECH.GRD_PRTF_TYPE_ENHC as
Select
   GPTA6.PERD_D                 as PERD_D
  ,GPTA6.PRTF_TYPE_C            as PRTF_TYPE_C
  ,GPTA6.PRTF_TYPE_M            as PRTF_TYPE_M
  ,GPCL6.PRTF_CLAS_C            as PRTF_CLAS_C
  ,GPCL6.PRTF_CLAS_M            as PRTF_CLAS_M
  ,GPCA6.PRTF_CATG_C            as PRTF_CATG_C
  ,GPCA6.PRTF_CATG_M            as PRTF_CATG_M
From  
  (
    Select
       MSIG.INT_GRUP_TYPE_C as PRTF_TYPE_C
      ,TIG.INT_GRUP_TYPE_M as PRTF_TYPE_M
      ,G.CLAS_SCHM_C
      ,G.PRTF_TYPE_NODE_C
      ,G.EFFT_D
      ,C.CALENDAR_DATE as PERD_D
    From
      PS_GDW1_BTEQ.PVTECH.GRD_PRTF_TYPE_ATTR G
      Inner Join PS_GDW1_BTEQ.PVTECH.MAP_SAP_INT_GRUP MSIG
      On MSIG.BUSN_PTNR_GRUP_TYPE = G.SAP_C
      And G.CLAS_SCHM_C = 'PRTF_TYPE'

      Inner Join PS_GDW1_BTEQ.PVTECH.TYPE_INT_GRUP TIG
      On TIG.INT_GRUP_TYPE_C = MSIG.INT_GRUP_TYPE_C

      Inner Join PS_GDW1_BTEQ.PVTECH.CALENDAR C
      On C.CALENDAR_DATE between G.NODE_EFFT_D and G.NODE_EXPY_D
      And C.CALENDAR_DATE between MSIG.EFFT_D and MSIG.EXPY_D
      And C.CALENDAR_DATE between TIG.EFFT_D and TIG.EXPY_D
      And C.CALENDAR_DATE between DATEADD(MONTH, -39, (CURRENT_DATE - EXTRACT(DAY FROM CURRENT_DATE) +1 ) ) and DATEADD(MONTH, 1, CURRENT_DATE)
      And G.EXPY_D = '9999-12-31'

    Where
      G.CLAS_SCHM_C = 'PRTF_TYPE'
    Qualify Rank() Over (Partition By G.PRTF_TYPE_NODE_C, C.CALENDAR_DATE
                          Order By G.EFFT_D Desc ) = 1
  ) as GPTA6

  Inner Join (
    Select
       D61.CLAS_SCHM_1_C
      ,D61.CLAS_SCHM_2_C
      ,D61.DIMN_NODE_1_C
      ,D61.DIMN_NODE_2_C
      ,D61.EFFT_D
      ,C.CALENDAR_DATE as PERD_D
    From
      PS_GDW1_BTEQ.PVTECH.DIMN_NODE_ASSC D61 
      Inner Join PS_GDW1_BTEQ.PVTECH.CALENDAR C
      On C.CALENDAR_DATE between D61.BUSN_EFFT_D AND D61.BUSN_EXPY_D
      And D61.EXPY_D = '9999-12-31'
      And C.CALENDAR_DATE between DATEADD(MONTH, -39, (CURRENT_DATE - EXTRACT(DAY FROM CURRENT_DATE) +1 ) ) and DATEADD(MONTH, 1, CURRENT_DATE)
    Where
        D61.CLAS_SCHM_1_C = 'PRTF_CLAS'
        And D61.DIMN_NODE_ASSC_TYPE_C = 'PRTF_TYPE_CLAS' 
    Qualify Rank() Over (Partition By D61.DIMN_NODE_1_C, D61.DIMN_NODE_2_C, C.CALENDAR_DATE
                          Order By D61.EFFT_D Desc ) = 1

  ) DNA61
  
  On DNA61.CLAS_SCHM_2_C = GPTA6.CLAS_SCHM_C
  And DNA61.DIMN_NODE_2_C = GPTA6.PRTF_TYPE_NODE_C
  And GPTA6.PERD_D = DNA61.PERD_D

  Inner Join (
    Select
       G.PRTF_CLAS_C
      ,G.PRTF_CLAS_M  
      ,G.CLAS_SCHM_C
      ,G.PRTF_CLAS_NODE_C
      ,G.EFFT_D
      ,C.CALENDAR_DATE as PERD_D
    From
      PS_GDW1_BTEQ.PVTECH.GRD_PRTF_CLAS_ATTR G
      Inner Join PS_GDW1_BTEQ.PVTECH.CALENDAR C
      On C.CALENDAR_DATE between G.NODE_EFFT_D AND G.NODE_EXPY_D 
      And G.EXPY_D = '9999-12-31'
      And C.CALENDAR_DATE between DATEADD(MONTH, -39, (CURRENT_DATE - EXTRACT(DAY FROM CURRENT_DATE) +1 ) ) and DATEADD(MONTH, 1, CURRENT_DATE)

    Qualify Rank() Over (Partition By G.PRTF_CLAS_NODE_C, C.CALENDAR_DATE
                          Order By G.EFFT_D Desc ) = 1

  ) as GPCL6
  On GPCL6.CLAS_SCHM_C = DNA61.CLAS_SCHM_1_C
  And GPCL6.PRTF_CLAS_NODE_C = DNA61.DIMN_NODE_1_C
  And GPCL6.PERD_D = DNA61.PERD_D

  Inner Join (  
    Select
       D62.CLAS_SCHM_1_C
      ,D62.CLAS_SCHM_2_C
      ,D62.DIMN_NODE_1_C
      ,D62.DIMN_NODE_2_C
      ,D62.EFFT_D
      ,C.CALENDAR_DATE as PERD_D
    From
      PS_GDW1_BTEQ.PVTECH.DIMN_NODE_ASSC D62 
      Inner Join PS_GDW1_BTEQ.PVTECH.CALENDAR C
      On C.CALENDAR_DATE between D62.BUSN_EFFT_D AND D62.BUSN_EXPY_D
      And D62.EXPY_D = '9999-12-31'
      And C.CALENDAR_DATE between DATEADD(MONTH, -39, (CURRENT_DATE - EXTRACT(DAY FROM CURRENT_DATE) +1 ) ) and DATEADD(MONTH, 1, CURRENT_DATE)
    Where
        D62.CLAS_SCHM_2_C = 'PRTF_CLAS'
        And D62.CLAS_SCHM_1_C = 'PRTF_CATG'
        And D62.DIMN_NODE_ASSC_TYPE_C = 'PRTF_CATG_CLAS' 

    Qualify Rank() Over (Partition By D62.DIMN_NODE_1_C, D62.DIMN_NODE_2_C, C.CALENDAR_DATE
                          Order By D62.EFFT_D Desc ) = 1

  ) as DNA62   

  On DNA62.CLAS_SCHM_2_C = DNA61.CLAS_SCHM_1_C
  And DNA62.DIMN_NODE_2_C = DNA61.DIMN_NODE_1_C
  And DNA62.PERD_D = DNA61.PERD_D

  Inner Join (
    Select
       G.PRTF_CATG_C
      ,G.PRTF_CATG_M
      ,G.CLAS_SCHM_C
      ,G.PRTF_CATG_NODE_C
      ,G.EFFT_D
      ,C.CALENDAR_DATE as PERD_D
    From
      PS_GDW1_BTEQ.PVTECH.GRD_PRTF_CATG_ATTR G
      Inner Join PS_GDW1_BTEQ.PVTECH.CALENDAR C
      On C.CALENDAR_DATE between G.NODE_EFFT_D AND G.NODE_EXPY_D 
      And G.EXPY_D = '9999-12-31'

      And C.CALENDAR_DATE between DATEADD(MONTH, -39, (CURRENT_DATE - EXTRACT(DAY FROM CURRENT_DATE) +1 ) ) and DATEADD(MONTH, 1, CURRENT_DATE)  

   Qualify Rank() Over (Partition By G.PRTF_CATG_NODE_C, C.CALENDAR_DATE
                          Order By G.EFFT_D Desc ) = 1
  
  ) as GPCA6

  On GPCA6.CLAS_SCHM_C =  DNA62.CLAS_SCHM_1_C
  And GPCA6.PRTF_CATG_NODE_C = DNA62.DIMN_NODE_1_C
  And GPCA6.PERD_D = DNA62.PERD_D

Group By 1,2,3,4,5,6,7

-- $LastChangedBy: jelifft $
-- $LastChangedDate: 2013-07-17 12:11:03 +1000 (Wed, 17 Jul 2013) $
-- $LastChangedRevision: 12304 $
;
