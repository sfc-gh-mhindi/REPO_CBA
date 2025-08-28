USE DATABASE NPD_D12_DMN_GDWMIG_IBRG_V;
 
-- DROP VIEW IF EXISTS PVCBODS.ODS_RULE;
 
/* <sc-view> PVCBODS.CBA_FNCL_SERV_GL_DATA_CURR </sc-view> */
CREATE OR REPLACE VIEW NPD_D12_DMN_GDWMIG_IBRG_V.PVCBODS.CBA_FNCL_SERV_GL_DATA_CURR
            (
                RECD_CNT,
                INTR_CNCT_ID,
                INTR_OBJC_IDNN,
                SRCE_SYST_ISAC_CODE,
                OBJC_BDT_APPT,
                OBJC_FUNC,
                CHNG_TIME_STMP,
                VALD_FROM,
                ACTL_VALD_END,
                LAST_CHNG_BY,
                BUSN_TRAN_CATG,
                SALE_PDCT,
                DEPT_ID,
                AFLT,
                ALT_PDCT_ID,
                SHRT_FORM_TIME_STMP,
                MIGR_GRUP,
                UPDT_MODE,
                LOAD_S,
                PROS_KEY_EFFT_I,
                ROW_SECU_ACCS_C,
                UPD_LOAD_S,
                UPD_PROS_KEY_EFFT_I,
                IS_CURR_IND
            )
            COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 16,  "patch": "2.0" }, "attributes": {  "component": "teradata",  "convertedOn": "08/21/2025",  "domain": "snowflake" }}'
            AS
SELECT
        RECD_CNT,
        INTR_CNCT_ID,
        INTR_OBJC_IDNN,
        SRCE_SYST_ISAC_CODE,
        OBJC_BDT_APPT,
        OBJC_FUNC,
        CHNG_TIME_STMP,
        VALD_FROM,
        ACTL_VALD_END,
        LAST_CHNG_BY,
        BUSN_TRAN_CATG,
        SALE_PDCT,
        DEPT_ID,
        AFLT,
        ALT_PDCT_ID,
        SHRT_FORM_TIME_STMP,
        MIGR_GRUP,
        UPDT_MODE,
        LOAD_S,
        PROS_KEY_EFFT_I,
        ROW_SECU_ACCS_C,
        UPD_LOAD_S,
        UPD_PROS_KEY_EFFT_I,
        IS_CURR_IND
         FROM
        PDCBODS.CBA_FNCL_SERV_GL_DATA
         WHERE  IS_CURR_IND = 1  /* Retrieve only the "Current" record per Primary Key */
 
        AND
 
       (
       (
       /* Start - RLS */
        COALESCE(ROW_SECU_ACCS_C,0) = 0 OR GETBIT( (
        SELECT
                    try_to_number(ROW_SECU_PRFL_C::varchar)  --Need to double check this logic
        FROM
                    ps_gdw1_bteq.PVSECURITY.ROW_LEVL_SECU_USER_PRFL
        WHERE
                    UPPER(RTRIM( username)) = UPPER(RTRIM(CURRENT_USER))
        ),ROW_SECU_ACCS_C
        ) = 1
       /* End - RLS */
       )
              );

CREATE OR REPLACE VIEW NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.ACCT_BASE
(
        ACCT_I,
        SRCE_SYST_ACCT_N,
        OPEN_D,
        ACCT_CLSE_REAS_C,
        CLSE_D,
        BUSN_UNIT_BRCH_N,
        ORIG_I,
        BUSN_UNIT_I,
        SRCE_SYST_C,
        ACCT_N,
        ACCT_QLFY_C,
        CNCY_C,
        ACCT_OBTN_TYPE_C,
        ACCT_CATG_TYPE_C,
        ACCT_CLAS_TYPE_C,
        PROS_KEY_EFFT_I,
        PROS_KEY_EXPY_I,
        EROR_SEQN_I,
        ROW_SECU_ACCS_C,
        ISO_CNTY_C,
        BANK_N,
        EFFT_D,
        EXPY_D,
        RECORD_DELETED_FLAG,
        CTL_ID,
        PROCESS_NAME,
        PROCESS_ID,
        UPDATE_PROCESS_NAME,
        UPDATE_PROCESS_ID
)
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 16,  "patch": "2.0" }, "attributes": {  "component": "teradata",  "convertedOn": "08/21/2025",  "domain": "snowflake" }}'
AS
SELECT
        ACCT_I,
        SRCE_SYST_ACCT_N,
        OPEN_D,
        ACCT_CLSE_REAS_C,
        CLSE_D,
        BUSN_UNIT_BRCH_N,
        ORIG_I,
        BUSN_UNIT_I,
        SRCE_SYST_C,
        ACCT_N,
        ACCT_QLFY_C,
        CNCY_C,
        ACCT_OBTN_TYPE_C,
        ACCT_CATG_TYPE_C,
        ACCT_CLAS_TYPE_C,
        PROS_KEY_EFFT_I,
        PROS_KEY_EXPY_I,
        EROR_SEQN_I,
        ROW_SECU_ACCS_C,
        ISO_CNTY_C,
        BANK_N,
        EFFT_D,
        EXPY_D,
        RECORD_DELETED_FLAG,
        CTL_ID,
        PROCESS_NAME,
        PROCESS_ID,
        UPDATE_PROCESS_NAME,
        UPDATE_PROCESS_ID
  FROM (
        SELECT
            ACCT_I,
            SRCE_SYST_ACCT_N,
            OPEN_D,
            ACCT_CLSE_REAS_C,
            CLSE_D,
            BUSN_UNIT_BRCH_N,
            ORIG_I,
            BUSN_UNIT_I,
            SRCE_SYST_C,
            ACCT_N,
            ACCT_QLFY_C,
            CNCY_C,
            ACCT_OBTN_TYPE_C,
            ACCT_CATG_TYPE_C,
            ACCT_CLAS_TYPE_C,
            PROS_KEY_EFFT_I,
            PROS_KEY_EXPY_I,
            EROR_SEQN_I,
            CASE
                WHEN ACCT_I ILIKE 'FMS%'
                    THEN 1 ELSE ROW_SECU_ACCS_C
            END AS ROW_SECU_ACCS_C,
            ISO_CNTY_C,
            BANK_N,
            EFFT_D,
            EXPY_D,
            RECORD_DELETED_FLAG,
            CTL_ID,
            PROCESS_NAME,
            PROCESS_ID,
            UPDATE_PROCESS_NAME,
            UPDATE_PROCESS_ID
FROM
            NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.ACCT_BASE
        ) ACCT_BASE
  WHERE
((
/* Start - RLS */
        COALESCE(ROW_SECU_ACCS_C,0) = 0 OR GETBIT( (
        SELECT
                    try_to_number(ROW_SECU_PRFL_C::varchar)  --Need to double check this logic
        FROM
                    ps_gdw1_bteq.PVSECURITY.ROW_LEVL_SECU_USER_PRFL
        WHERE
                    UPPER(RTRIM( username)) = UPPER(RTRIM(CURRENT_USER))
        ),ROW_SECU_ACCS_C
        ) = 1
/* End - RLS */
));
 
/* <sc-table> NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.ACCT_BASE </sc-table> */
                        --** SSC-FDM-TD0024 - SET TABLE FUNCTIONALITY NOT SUPPORTED. TABLE MIGHT HAVE DUPLICATE ROWS **

CREATE OR REPLACE VIEW NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.ACCT_OFFR
(
        ACCT_I,
        OFFR_I,
        EFFT_D,
        SRCE_SYST_C,
        EXPY_D,
        PROS_KEY_EFFT_I,
        PROS_KEY_EXPY_I,
        ROW_SECU_ACCS_C,
        ACCT_OFFR_STRT_D,
        ACCT_OFFR_END_D
)
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 16,  "patch": "2.0" }, "attributes": {  "component": "teradata",  "convertedOn": "08/21/2025",  "domain": "snowflake" }}'
AS
SELECT
        ACCT_I,
        OFFR_I,
        EFFT_D,
        SRCE_SYST_C,
        EXPY_D,
        PROS_KEY_EFFT_I,
        PROS_KEY_EXPY_I,
        ROW_SECU_ACCS_C,
        ACCT_OFFR_STRT_D,
        ACCT_OFFR_END_D
  FROM
        NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.ACCT_OFFR
WHERE
(
/* Start - RLS */
        COALESCE(ROW_SECU_ACCS_C,0) = 0 OR GETBIT( (
        SELECT
                    try_to_number(ROW_SECU_PRFL_C::varchar)  --Need to double check this logic
        FROM
                    ps_gdw1_bteq.PVSECURITY.ROW_LEVL_SECU_USER_PRFL
        WHERE
                    UPPER(RTRIM( username)) = UPPER(RTRIM(CURRENT_USER))
        ),ROW_SECU_ACCS_C
        ) = 1
/* End - RLS */
);
 
/* <sc-table> NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.ACCT_OFFR </sc-table> */
                        --** SSC-FDM-TD0024 - SET TABLE FUNCTIONALITY NOT SUPPORTED. TABLE MIGHT HAVE DUPLICATE ROWS **

CREATE OR REPLACE VIEW NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.ACCT_PATY
            (
                    PATY_I,
                    ACCT_I,
                    PATY_ACCT_REL_C,
                    REL_LEVL_C,
                    REL_REAS_C,
                    REL_STUS_C,
                    SRCE_SYST_C,
                    EFFT_D,
                    EXPY_D,
                    PROS_KEY_EFFT_I,
                    PROS_KEY_EXPY_I,
                    ROW_SECU_ACCS_C
            )
            COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 16,  "patch": "2.0" }, "attributes": {  "component": "teradata",  "convertedOn": "08/21/2025",  "domain": "snowflake" }}'
            AS
SELECT
        PATY_I,
        ACCT_I,
        PATY_ACCT_REL_C,
        REL_LEVL_C,
        REL_REAS_C,
        REL_STUS_C,
        SRCE_SYST_C,
        EFFT_D,
        EXPY_D,
        PROS_KEY_EFFT_I,
        PROS_KEY_EXPY_I,
        ROW_SECU_ACCS_C
  FROM (
        SELECT
            PATY_I,
            ACCT_I,
            PATY_ACCT_REL_C,
            REL_LEVL_C,
            REL_REAS_C,
            REL_STUS_C,
            SRCE_SYST_C,
            EFFT_D,
            EXPY_D,
            PROS_KEY_EFFT_I,
            PROS_KEY_EXPY_I,
            CASE
                WHEN ACCT_I ILIKE 'FMS%'
                    THEN 1 ELSE ROW_SECU_ACCS_C
            END AS ROW_SECU_ACCS_C
FROM
            PDPATY.ACCT_PATY
        ) ACCT_PATY
WHERE
        (
/* Start - RLS */
        COALESCE(ROW_SECU_ACCS_C,0) = 0 OR GETBIT( (
        SELECT
                    try_to_number(ROW_SECU_PRFL_C::varchar)  --Need to double check this logic
        FROM
                    ps_gdw1_bteq.PVSECURITY.ROW_LEVL_SECU_USER_PRFL
        WHERE
                    UPPER(RTRIM( username)) = UPPER(RTRIM(CURRENT_USER))
        ),ROW_SECU_ACCS_C
        ) = 1
/* End - RLS */
);

CREATE OR REPLACE VIEW NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.ACCT_PDCT
            (
                    ACCT_I,
                    EFFT_D,
                    EXPY_D,
                    PDCT_N,
                    PROS_KEY_EFFT_I,
                    PROS_KEY_EXPY_I,
                    EROR_SEQN_I,
                    ROW_SECU_ACCS_C,
                    MFTR_PDCT_I,
                    SRCE_SYST_C,
                    RECORD_DELETED_FLAG,
                    CTL_ID,
                    PROCESS_NAME,
                    PROCESS_ID,
                    UPDATE_PROCESS_NAME,
                    UPDATE_PROCESS_ID
            )
            COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 16,  "patch": "2.0" }, "attributes": {  "component": "teradata",  "convertedOn": "08/21/2025",  "domain": "snowflake" }}'
            AS
SELECT
        ACCT_I,
        EFFT_D,
        EXPY_D,
        PDCT_N,
        PROS_KEY_EFFT_I,
        PROS_KEY_EXPY_I,
        EROR_SEQN_I,
        ROW_SECU_ACCS_C,
        MFTR_PDCT_I,
        SRCE_SYST_C,
        RECORD_DELETED_FLAG,
        CTL_ID,
        PROCESS_NAME,
        PROCESS_ID,
        UPDATE_PROCESS_NAME,
        UPDATE_PROCESS_ID
  FROM (
        SELECT
            ACCT_I,
            EFFT_D,
            EXPY_D,
            PDCT_N,
            PROS_KEY_EFFT_I,
            PROS_KEY_EXPY_I,
            EROR_SEQN_I,
            CASE
                WHEN ACCT_I ILIKE 'FMS%'
                    THEN 1 ELSE ROW_SECU_ACCS_C
            END AS ROW_SECU_ACCS_C,
            MFTR_PDCT_I,
            SRCE_SYST_C,
            RECORD_DELETED_FLAG,
            CTL_ID,
            PROCESS_NAME,
            PROCESS_ID,
            UPDATE_PROCESS_NAME,
            UPDATE_PROCESS_ID
FROM
            NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.ACCT_PDCT
        ) ACCT_PDCT
  WHERE
RECORD_DELETED_FLAG = 0
AND ( (
/* Start - RLS */
        COALESCE(ROW_SECU_ACCS_C,0) = 0 OR GETBIT( (
        SELECT
                    try_to_number(ROW_SECU_PRFL_C::varchar)  --Need to double check this logic
        FROM
                    ps_gdw1_bteq.PVSECURITY.ROW_LEVL_SECU_USER_PRFL
        WHERE
                    UPPER(RTRIM( username)) = UPPER(RTRIM(CURRENT_USER))
        ),ROW_SECU_ACCS_C
        ) = 1
/* End - RLS */
));
 
/* <sc-view> PVTECH.DERV_PRTF_ACCT </sc-view> */
CREATE OR REPLACE VIEW NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_ACCT
            -- --- ----------
            -- Ver Date       Modified By         Description
            -- --- ----------
            --
            -- 1.0  26/06/2013 T Jelliffe          Initial Version
            -- 1.1  28/06/2013 T Jelliffe          Use duration persist table
            -- 1.2  12/07/2013 T Jelliffe          Time period reduced 15 to 3 years
            -- 1.3  17/07/2013 T Jelliffe          39 months history range
            -- 1.4  25/07/2013 T Jelliffe          Date join on JOIN_FROM_D and TO_D
            -- 1.5  14/01/2014 H Zak               read from the corresponding 1:1 views over the new relationship tables
            (
             PERD_D
            ,VALD_FROM_D
            ,VALD_TO_D
            ,EFFT_D
            ,EXPY_D
            ,ACCT_I
            ,INT_GRUP_I
            ,DERV_PRTF_CATG_C
            ,DERV_PRTF_CLAS_C
            ,DERV_PRTF_TYPE_C
            ,PTCL_N
            ,REL_MNGE_I
            ,PRTF_CODE_X
            ,SRCE_SYST_C
            ,ROW_SECU_ACCS_C