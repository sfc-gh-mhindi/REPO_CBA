use role r_dev_npd_d12_gdwmig;
use database NPD_D12_DMN_GDWMIG_IBRG;
use warehouse wh_usr_npd_d12_gdwmig_001;
use schema iceberg_migrator;

USE DATABASE NPD_D12_DMN_GDWMIG_IBRG_V;


CREATE OR REPLACE VIEW NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.GRD_RPRT_CALR_CLYR
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 16,  "patch": "1.0" }, "attributes": {  "component": "teradata",  "convertedOn": "08/22/2025",  "domain": "snowflake" }}'
AS
--** SSC-FDM-0001 - VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
SELECT
 *
FROM
 NPD_D12_DMN_GDWMIG_IBRG.PDGRD.GRD_RPRT_CALR_CLYR;


 CREATE OR REPLACE VIEW NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.ACCT_BALN_ADJ (
ACCT_I,
  SRCE_SYST_C,
  BALN_TYPE_C,
  CALC_FUNC_C,
  TIME_PERD_C,
  ADJ_FROM_D,
  ADJ_TO_D,
  ADJ_A,
  GL_RECN_F,
  EFFT_D,
  PROS_KEY_EFFT_I,
  EROR_SEQN_I,
  CNCY_C,
  CALC_F,
  ROW_SECU_ACCS_C
)
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 16,  "patch": "1.0" }, "attributes": {  "component": "teradata",  "convertedOn": "08/22/2025",  "domain": "snowflake" }}'
AS
SELECT
 ACCT_I,
SRCE_SYST_C,
BALN_TYPE_C,
CALC_FUNC_C,
TIME_PERD_C,
ADJ_FROM_D,
ADJ_TO_D,
 CASE
  WHEN (UPPER(RTRIM(SRCE_SYST_C)) = UPPER(RTRIM('SAP')) AND (UPPER(RTRIM(CNCY_C)) = UPPER(RTRIM('AUD')) OR CNCY_C IS NULL)) OR (UPPER(RTRIM(COALESCE(SRCE_SYST_C,''))) <> UPPER(RTRIM('SAP')))
   THEN CAST(ADJ_A AS DECIMAL (15,2))
 END AS ADJ_A,
GL_RECN_F,
EFFT_D,
PROS_KEY_EFFT_I,
EROR_SEQN_I,
CNCY_C,
CALC_F,
ROW_SECU_ACCS_C
FROM
 NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.ACCT_BALN_ADJ
WHERE (UPPER(RTRIM(SRCE_SYST_C)) = UPPER(RTRIM('SAP')) AND (UPPER(RTRIM(CNCY_C)) = UPPER(RTRIM('AUD')) OR CNCY_C is NULL))
OR (UPPER(RTRIM(COALESCE(SRCE_SYST_C,''))) <> UPPER(RTRIM('SAP')))

AND (
(
/* Start - RLS */
 COALESCE(ROW_SECU_ACCS_C,0) = 0 OR GETBIT( (
	SELECT
             try_to_number(ROW_SECU_PRFL_C::varchar)  --Need to double check this logic
FROM
             NPD_D12_DMN_GDWMIG_IBRG_V.PVSECURITY.ROW_LEVL_SECU_USER_PRFL
WHERE
             UPPER(RTRIM( username)) = UPPER(RTRIM(CURRENT_USER))
),ROW_SECU_ACCS_C
) = 1
/* End - RLS */
)
); 

//// apply on demo account ////
USE DATABASE NPD_D12_DMN_GDWMIG_IBRG_V;
 
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
                        NPD_D12_DMN_GDWMIG_IBRG_V.PVSECURITY.ROW_LEVL_SECU_USER_PRFL
        WHERE
                        UPPER(RTRIM( username)) = UPPER(RTRIM(CURRENT_USER))
),ROW_SECU_ACCS_C
) = 1
/* End - RLS */
));
 
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
                        NPD_D12_DMN_GDWMIG_IBRG_V.PVSECURITY.ROW_LEVL_SECU_USER_PRFL
        WHERE
                        UPPER(RTRIM( username)) = UPPER(RTRIM(CURRENT_USER))
),ROW_SECU_ACCS_C
) = 1
/* End - RLS */
);
 
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
                        NPD_D12_DMN_GDWMIG_IBRG.PDPATY.ACCT_PATY
        ) ACCT_PATY
WHERE
        (
/* Start - RLS */
        COALESCE(ROW_SECU_ACCS_C,0) = 0 OR GETBIT( (
                SELECT
                        try_to_number(ROW_SECU_PRFL_C::varchar)  --Need to double check this logic
        FROM
                        NPD_D12_DMN_GDWMIG_IBRG_V.PVSECURITY.ROW_LEVL_SECU_USER_PRFL
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
                        NPD_D12_DMN_GDWMIG_IBRG_V.PVSECURITY.ROW_LEVL_SECU_USER_PRFL
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
 
)
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 16,  "patch": "2.0" }, "attributes": {  "component": "teradata",  "convertedOn": "08/21/2025",  "domain": "snowflake" }}'
AS
SELECT
        CALR.CALENDAR_DATE AS PERD_D,
        VALD_FROM_D,
        VALD_TO_D,
        EFFT_D,
        EXPY_D,
        ACCT_I,
        INT_GRUP_I,
        DERV_PRTF_CATG_C,
        DERV_PRTF_CLAS_C,
        DERV_PRTF_TYPE_C,
        PTCL_N,
        REL_MNGE_I,
        PRTF_CODE_X,
        SRCE_SYST_C,
        ROW_SECU_ACCS_C
      FROM
        PVTECH.DERV_PRTF_ACCT_REL T1
        INNER JOIN
        PVTECH.CALENDAR CALR
        ON CALR.CALENDAR_DATE BETWEEN T1.VALD_FROM_D AND T1.VALD_TO_D
        AND CALR.CALENDAR_DATE BETWEEN DATEADD(MONTH, -39, (CURRENT_DATE() - EXTRACT(DAY FROM CURRENT_DATE()) +1 ))
        AND DATEADD(MONTH, 1, CURRENT_DATE())
        ;
 
/* <sc-view> PVTECH.DERV_PRTF_ACCT_REL </sc-view> */
CREATE OR REPLACE VIEW NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_ACCT_REL
(
        ACCT_I,
         INT_GRUP_I,
         DERV_PRTF_CATG_C,
         DERV_PRTF_CLAS_C,
         DERV_PRTF_TYPE_C,
         VALD_FROM_D,
         VALD_TO_D,
         EFFT_D,
         EXPY_D,
         PTCL_N,
         REL_MNGE_I,
         PRTF_CODE_X,
         SRCE_SYST_C,
         ROW_SECU_ACCS_C
)
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 16,  "patch": "2.0" }, "attributes": {  "component": "teradata",  "convertedOn": "08/21/2025",  "domain": "snowflake" }}'
AS
SELECT
        ACCT_I,
INT_GRUP_I,
DERV_PRTF_CATG_C,
DERV_PRTF_CLAS_C,
DERV_PRTF_TYPE_C,
VALD_FROM_D,
VALD_TO_D,
EFFT_D,
EXPY_D,
PTCL_N,
REL_MNGE_I,
PRTF_CODE_X,
SRCE_SYST_C,
ROW_SECU_ACCS_C
  FROM
        NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_PRTF_ACCT_REL
 
WHERE (
(
/* Start - RLS */
        COALESCE(ROW_SECU_ACCS_C,0) = 0 OR GETBIT( (
                SELECT
                        try_to_number(ROW_SECU_PRFL_C::varchar)  --Need to double check this logic
        FROM
                        NPD_D12_DMN_GDWMIG_IBRG_V.PVSECURITY.ROW_LEVL_SECU_USER_PRFL
        WHERE
                        UPPER(RTRIM( username)) = UPPER(RTRIM(CURRENT_USER))
),ROW_SECU_ACCS_C
) = 1
/* End - RLS */
)
    );
 
/* <sc-view> PVTECH.CALENDAR </sc-view> */
CREATE OR REPLACE VIEW NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.CALENDAR
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 16,  "patch": "2.0" }, "attributes": {  "component": "teradata",  "convertedOn": "08/21/2025",  "domain": "snowflake" }}'
AS
--** SSC-FDM-0001 - VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
SELECT
        *
FROM SYS_CALENDAR.CALENDAR;
 
/* Re-place the existing views */
CREATE OR REPLACE VIEW NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_PATY_PSST
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 16,  "patch": "2.0" }, "attributes": {  "component": "teradata",  "convertedOn": "08/21/2025",  "domain": "snowflake" }}'
AS
SELECT
        INT_GRUP_I,
        PATY_I,
        JOIN_FROM_D,
        JOIN_TO_D,
        EFFT_D,
        EXPY_D,
        VALD_FROM_D,
        VALD_TO_D,
        REL_C,
        SRCE_SYST_C,
        ROW_SECU_ACCS_C,
        PROS_KEY_I
     FROM
        NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_PRTF_PATY_PSST
        -- $LastChangedBy: jelifft $
        -- $LastChangedDate: 2013-10-21 16:39:07 +1100 (Mon, 21 Oct 2013) $
        -- $LastChangedRevision: 12832 $
        ;
 
/* <sc-view> PVTECH.GRD_RPRT_CALR_FNYR </sc-view> */
CREATE OR REPLACE VIEW NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.GRD_RPRT_CALR_FNYR
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 16,  "patch": "2.0" }, "attributes": {  "component": "teradata",  "convertedOn": "08/21/2025",  "domain": "snowflake" }}'
AS
--** SSC-FDM-0001 - VIEWS SELECTING ALL COLUMNS FROM A SINGLE TABLE ARE NOT REQUIRED IN SNOWFLAKE AND MAY IMPACT PERFORMANCE. **
SELECT
        *
FROM
        NPD_D12_DMN_GDWMIG_IBRG.PDGRD.GRD_RPRT_CALR_FNYR;
 
/* <sc-view> PVTECH.MAP_SAP_INVL_PDCT </sc-view> */
CREATE OR REPLACE VIEW NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.MAP_SAP_INVL_PDCT
(
        PDCT,
        PDCT_C,
        EFFT_D,
        EXPY_D
)
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 16,  "patch": "2.0" }, "attributes": {  "component": "teradata",  "convertedOn": "08/21/2025",  "domain": "snowflake" }}'
AS
SELECT
        PDCT,
        PDCT_C,
        EFFT_D,
        EXPY_D
  FROM
        NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_SAP_INVL_PDCT
        ;


CREATE OR REPLACE VIEW NPD_D12_DMN_GDWMIG_IBRG_V.pvcbods.BUSN_PTNR
(
        BUSN_PTNR_NUMB,
        SRCE_SYST_ISAC_CODE,
        PATY_I,
        SAP_PATY_I,
        DOB,
        FRST_ACDM_TITL,
        SCND_ACDM_TITL,
        BUSN_PTNR_NAME_AT_BRTH,
        BUSN_PTNR_CATG,
        BUSN_PTNR_CTCT_PMIS,
        BUSN_PTNR_NUMB_IN_EXTL_SYST,
        BUSN_PTNR_FRST_NAME,
        BUSN_PTNR_GRUP,
        BUSN_PTNR_NAME_1_GRUP,
        BUSN_PTNR_NAME_2_GRUP,
        BUSN_PTNR_GRUP_TYPE,
        BUSN_PTNR_SRNM,
        BUSN_PTNR_MRST,
        BUSN_PTNR_MDLE_NAME,
        BUSN_PTNR_SCND_SRNM,
        PTNR_TYPE,
        CHNG_BY,
        LAST_CHNG_AT,
        LAST_CHNG_ON,
        NAME_OF_PERS_WHO_CRAT_THE_OBJC,
        RECD_CRAT_DATE,
        TIME_CRAT,
        SLCT_GNDR_MALE,
        SLCT_GNDR_FEM,
        SLCT_GNDR_UNKN,
        INDC,
        LEGL_STUS_OF_ORGN,
        BUSN_PTNR_ORGN_NAME_1,
        BUSN_PTNR_ORGN_NAME_2,
        BUSN_PTNR_ORGN_NAME_3,
        BUSN_PTNR_ORGN_NAME_4,
        NATY,
        NAME_SUPT,
        FORM_OF_ADRS_TITL,
        CNTY_FOR_NAME_FRMT_RULE,
        SCND_NAME_PREF,
        DATE_ORGN_FOUN,
        BUSN_PTNR_CORR_LANG,
        BUSN_PTNR_LANG,
        NAME_FRMT,
        DATE_OF_DETH,
        OCCP,
        UPDT_MODE,
        LOAD_S,
        PROS_KEY_EFFT_I,
        ROW_SECU_ACCS_C,
        RECD_CNT,
        ADRS_NUMB,
        BUSN_PTNR_BRTH_PLAC,
        SRCH_TERM_1,
        SRCH_TERM_2,
        CNTY_OF_ORIG,
        CNTY_OF_TAX,
        NAME_OF_EMPR_OF_NATU_PERS,
        MDLE_INLS,
        LENT_FOR_BUSN_PTNR,
        CLTL_OBJC_LIQD_DATE,
        INTL_LOCN_NUMB_PART_1,
        INTL_LOCN_NUMB_PART_2,
        INTL_LOCN_NUMB_PART_3,
        SRCH_HELP_1_NAME_1_LAST_NAME,
        SRCH_HELP_2_NAME_2_FRST_NAME,
        NAME,
        NAME_PART_2,
        BUSN_PTNR_IS_A_NATU_PERS_INDA,
        NCKN,
        GuID_OF_BUSN_PTNR,
        PERS,
        ORDR_PATY_PERS_NUMB,
        NAME_SUPT_PREF_1,
        PRNT_MODE,
        DATA_SRCE,
        PLAN_CHNG_DOCU_FOR_PTNR_CONV,
        SALU,
        VALD_FROM_UTC_DTTS,
        VALD_TO_UTC_DTTS,
        BUSN_PTNR_BLOK,
        BUSN_PTNR_DELT_FLAG,
        AUTN_GRUP,
        UPD_LOAD_S,
        UPD_PROS_KEY_EFFT_I,
        IS_CURR_IND
)
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 16,  "patch": "2.0" }, "attributes": {  "component": "teradata",  "convertedOn": "08/21/2025",  "domain": "snowflake" }}'
AS
SELECT
        BUSN_PTNR_NUMB,
        SRCE_SYST_ISAC_CODE,
        PATY_I,
        SAP_PATY_I,
        DOB,
        '***HIDDEN (FRST_ACDM_TITL)***',
        '***HIDDEN (SCND_ACDM_TITL)***',
        '***HIDDEN (BUSN_PTNR_NAME_AT_BRTH)***',
        BUSN_PTNR_CATG,
        BUSN_PTNR_CTCT_PMIS,
        BUSN_PTNR_NUMB_IN_EXTL_SYST,
        '***HIDDEN (BUSN_PTNR_FRST_NAME)***',
        BUSN_PTNR_GRUP,
        '***HIDDEN (BUSN_PTNR_NAME_1_GRUP)***',
        '***HIDDEN (BUSN_PTNR_NAME_2_GRUP)***',
        BUSN_PTNR_GRUP_TYPE,
        '***HIDDEN (BUSN_PTNR_SRNM)***',
        BUSN_PTNR_MRST,
        '***HIDDEN (BUSN_PTNR_MDLE_NAME)***',
        '***HIDDEN (BUSN_PTNR_SCND_SRNM)***',
        PTNR_TYPE,
        CHNG_BY,
        LAST_CHNG_AT,
        LAST_CHNG_ON,
        NAME_OF_PERS_WHO_CRAT_THE_OBJC,
        RECD_CRAT_DATE,
        TIME_CRAT,
        SLCT_GNDR_MALE,
        SLCT_GNDR_FEM,
        SLCT_GNDR_UNKN,
        INDC,
        LEGL_STUS_OF_ORGN,
        '***HIDDEN (BUSN_PTNR_ORGN_NAME_1)***',
        '***HIDDEN (BUSN_PTNR_ORGN_NAME_2)***',
        '***HIDDEN (BUSN_PTNR_ORGN_NAME_3)***',
        '***HIDDEN (BUSN_PTNR_ORGN_NAME_4)***',
        NATY,
        NAME_SUPT,
        FORM_OF_ADRS_TITL,
        CNTY_FOR_NAME_FRMT_RULE,
        SCND_NAME_PREF,
        DATE_ORGN_FOUN,
        BUSN_PTNR_CORR_LANG,
        BUSN_PTNR_LANG,
        NAME_FRMT,
        DATE_OF_DETH,
        OCCP,
        UPDT_MODE,
        LOAD_S,
        PROS_KEY_EFFT_I,
        ROW_SECU_ACCS_C,
        RECD_CNT,
        ADRS_NUMB,
        BUSN_PTNR_BRTH_PLAC,
        SRCH_TERM_1,
        SRCH_TERM_2,
        CNTY_OF_ORIG,
        CNTY_OF_TAX,
        NAME_OF_EMPR_OF_NATU_PERS,
        MDLE_INLS,
        LENT_FOR_BUSN_PTNR,
        CLTL_OBJC_LIQD_DATE,
        INTL_LOCN_NUMB_PART_1,
        INTL_LOCN_NUMB_PART_2,
        INTL_LOCN_NUMB_PART_3,
           '***HIDDEN (SRCH_HELP_1_NAME_1_LAST_NAME)***',
        '***HIDDEN (SRCH_HELP_2_NAME_2_FRST_NAME)***',
           '***HIDDEN (NAME)***',
        '***HIDDEN (NAME_PART_2)***',
        BUSN_PTNR_IS_A_NATU_PERS_INDA,
        '***HIDDEN (NCKN)***',
        GuID_OF_BUSN_PTNR,
        PERS,
        ORDR_PATY_PERS_NUMB,
        NAME_SUPT_PREF_1,
        PRNT_MODE,
        DATA_SRCE,
        PLAN_CHNG_DOCU_FOR_PTNR_CONV,
        SALU,
        VALD_FROM_UTC_DTTS,
        VALD_TO_UTC_DTTS,
        BUSN_PTNR_BLOK,
        BUSN_PTNR_DELT_FLAG,
        AUTN_GRUP,
        UPD_LOAD_S,
        UPD_PROS_KEY_EFFT_I,
        IS_CURR_IND
         FROM
        NPD_D12_DMN_GDWMIG_IBRG.PDCBODS.BUSN_PTNR
 
       WHERE
       (
       /* Start - RLS */
        COALESCE(ROW_SECU_ACCS_C,0) = 0 OR GETBIT( (
                SELECT
                        try_to_number(ROW_SECU_PRFL_C::varchar)  --Need to double check this logic
        FROM
                        NPD_D12_DMN_GDWMIG_IBRG_V.PVSECURITY.ROW_LEVL_SECU_USER_PRFL
        WHERE
                        UPPER(RTRIM( username)) = UPPER(RTRIM(CURRENT_USER))
       ),ROW_SECU_ACCS_C
       ) = 1
       /* End - RLS */
       );
 
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
        NPD_D12_DMN_GDWMIG_IBRG.PDCBODS.CBA_FNCL_SERV_GL_DATA
         WHERE  IS_CURR_IND = 1  /* Retrieve only the "Current" record per Primary Key */
 
        AND
 
       (
       (
       /* Start - RLS */
        COALESCE(ROW_SECU_ACCS_C,0) = 0 OR GETBIT( (
                SELECT
                        try_to_number(ROW_SECU_PRFL_C::varchar)  --Need to double check this logic
        FROM
                        NPD_D12_DMN_GDWMIG_IBRG_V.PVSECURITY.ROW_LEVL_SECU_USER_PRFL
        WHERE
                        UPPER(RTRIM( username)) = UPPER(RTRIM(CURRENT_USER))
       ),ROW_SECU_ACCS_C
       ) = 1
       /* End - RLS */
       )
       );
 
/* <sc-view> PVCBODS.MSTR_CNCT_BALN_TRNF_PRTP </sc-view> */
CREATE OR REPLACE VIEW NPD_D12_DMN_GDWMIG_IBRG_V.PVCBODS.MSTR_CNCT_BALN_TRNF_PRTP
(
        RECD_CNT,
        INTR_CNCT_NUMB_OF_MAIN_CNCT,
        ELEM_OF_A_CNCT_HIER_GRUP,
        SRCE_SYST_ISAC_CODE,
        OBJC_BDT_APPT,
        INTR_OBJC_IDNN,
        OBJC_FUNC,
        TREE_TYPE,
        CNCT_CATG,
        MAIN_CNCT_CATG,
        VALD_FROM,
        ACTL_VALD_END,
        MEMB_BDT_APPT,
        OBJC_FIX_PRTP_MAIN_CNCT,
        HIER_ID,
        BALN_TRNF_ROLE,
        TSHD_AMT,
        CNCY,
        FUND_ACCT_RANK,
        MAX_PERC_RATE,
        BALN_TRNF_BALN_DETR_BSIS,
        SHRT_FORM_TIME_STMP,
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
        INTR_CNCT_NUMB_OF_MAIN_CNCT,
        ELEM_OF_A_CNCT_HIER_GRUP,
        SRCE_SYST_ISAC_CODE,
        OBJC_BDT_APPT,
        INTR_OBJC_IDNN,
        OBJC_FUNC,
        TREE_TYPE,
        CNCT_CATG,
        MAIN_CNCT_CATG,
        VALD_FROM,
        ACTL_VALD_END,
        MEMB_BDT_APPT,
        OBJC_FIX_PRTP_MAIN_CNCT,
        HIER_ID,
        BALN_TRNF_ROLE,
        TSHD_AMT,
        CNCY,
        FUND_ACCT_RANK,
        MAX_PERC_RATE,
        BALN_TRNF_BALN_DETR_BSIS,
        SHRT_FORM_TIME_STMP,
        UPDT_MODE,
        LOAD_S,
        PROS_KEY_EFFT_I,
        ROW_SECU_ACCS_C,
        UPD_LOAD_S,
        UPD_PROS_KEY_EFFT_I,
        IS_CURR_IND
         FROM
        NPD_D12_DMN_GDWMIG_IBRG.PDCBODS.MSTR_CNCT_BALN_TRNF_PRTP
 
       WHERE
       (
       /* Start - RLS */
        COALESCE(ROW_SECU_ACCS_C,0) = 0 OR GETBIT( (
                SELECT
                        try_to_number(ROW_SECU_PRFL_C::varchar)  --Need to double check this logic
        FROM
                        NPD_D12_DMN_GDWMIG_IBRG_V.PVSECURITY.ROW_LEVL_SECU_USER_PRFL
        WHERE
                        UPPER(RTRIM( username)) = UPPER(RTRIM(CURRENT_USER))
       ),ROW_SECU_ACCS_C
       ) = 1
       /* End - RLS */
       );
 
/* <sc-view> PVCBODS.MSTR_CNCT_MSTR_DATA_GENL </sc-view> */
CREATE OR REPLACE VIEW NPD_D12_DMN_GDWMIG_IBRG_V.PVCBODS.MSTR_CNCT_MSTR_DATA_GENL
(
RECD_CNT,
INTR_CNCT_NUMB_OF_MAIN_CNCT,
SRCE_SYST_ISAC_CODE,
ACCT_I,
OBJC_BDT_APPT,
CHNG_TIME_STMP,
VALD_STRT,
ACTL_VALD_END,
LAST_CHNG_BY,
BUSN_TRAN_CATG_FOR_THE_CHNG,
MSTR_CNCT_NUMB,
SHRT_FORM_TIME_STMP,
UPDT_MODE,
LOAD_S,
PROS_KEY_EFFT_I,
ROW_SECU_ACCS_C,
ORGN_UNIT,
CNCT_CLAR_1,
CNCT_CLAR_2,
CNCT_CLAR_3,
BANK_POST_AREA
)
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 16,  "patch": "2.0" }, "attributes": {  "component": "teradata",  "convertedOn": "08/21/2025",  "domain": "snowflake" }}'
AS
SELECT
        RECD_CNT,
        INTR_CNCT_NUMB_OF_MAIN_CNCT,
        SRCE_SYST_ISAC_CODE,
        ACCT_I,
        OBJC_BDT_APPT,
        CHNG_TIME_STMP,
        VALD_STRT,
        ACTL_VALD_END,
        LAST_CHNG_BY,
        BUSN_TRAN_CATG_FOR_THE_CHNG,
        MSTR_CNCT_NUMB,
        SHRT_FORM_TIME_STMP,
        UPDT_MODE,
        LOAD_S,
        PROS_KEY_EFFT_I,
        ROW_SECU_ACCS_C,
        ORGN_UNIT,
        CNCT_CLAR_1,
        CNCT_CLAR_2,
        CNCT_CLAR_3,
        BANK_POST_AREA
         FROM
        NPD_D12_DMN_GDWMIG_IBRG.PDCBODS.MSTR_CNCT_MSTR_DATA_GENL
 
       WHERE
       (
      /* Start - RLS */
        COALESCE(ROW_SECU_ACCS_C,0) = 0 OR GETBIT( (
                SELECT
                        try_to_number(ROW_SECU_PRFL_C::varchar)  --Need to double check this logic
        FROM
                        NPD_D12_DMN_GDWMIG_IBRG_V.PVSECURITY.ROW_LEVL_SECU_USER_PRFL
        WHERE
                        UPPER(RTRIM( username)) = UPPER(RTRIM(CURRENT_USER))
      ),ROW_SECU_ACCS_C
      ) = 1
      /* End - RLS */
      )
 
                -- $LastChangedBy: pavithra$
                -- $LastChangedDate: $
                -- $LastChangedRevision: 6006$
                ;
 
/* <sc-view> PVCBODS.MSTR_CNCT_PRXY_ACCT </sc-view> */
CREATE OR REPLACE VIEW NPD_D12_DMN_GDWMIG_IBRG_V.PVCBODS.MSTR_CNCT_PRXY_ACCT
(
        RECD_CNT,
        ELEM_OF_A_CNCT_HIER_GRUP,
        CNCT_TYPE_KEY,
        SRCE_SYST_ISAC_CODE,
        EXTL_CNCT_PART_1,
        EXTL_CNCT_PART_2,
        EXTL_CNCT_PART_3,
        ACCT_CATG,
        UPDT_MODE,
        LOAD_S,
        PROS_KEY_EFFT_I,
        ROW_SECU_ACCS_C,
        NON_SAP_ACCT_IDNN,
        NON_SAP_ACCT_SALE_PDCT,
        NON_SAP_ACCT_NUMB,
        CIF_PDCT_CODE,
        CIF_SUB_PDCT_CODE,
        APPT_CODE,
        SRCE_SYST,
        CIF_PDCT_IDNN,
        CRIS_PDCT_C
)
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 16,  "patch": "2.0" }, "attributes": {  "component": "teradata",  "convertedOn": "08/21/2025",  "domain": "snowflake" }}'
AS
SELECT
        RECD_CNT,
        ELEM_OF_A_CNCT_HIER_GRUP,
        CNCT_TYPE_KEY,
        SRCE_SYST_ISAC_CODE,
        EXTL_CNCT_PART_1,
        EXTL_CNCT_PART_2,
        EXTL_CNCT_PART_3,
        ACCT_CATG,
        UPDT_MODE,
        LOAD_S,
        PROS_KEY_EFFT_I,
        ROW_SECU_ACCS_C,
        NON_SAP_ACCT_IDNN,
        NON_SAP_ACCT_SALE_PDCT,
        NON_SAP_ACCT_NUMB,
        CIF_PDCT_CODE,
        CIF_SUB_PDCT_CODE,
        APPT_CODE,
        SRCE_SYST,
        CIF_PDCT_IDNN,
        CRIS_PDCT_C
         FROM
        NPD_D12_DMN_GDWMIG_IBRG.PDCBODS.MSTR_CNCT_PRXY_ACCT
 
       WHERE
       (
       /* Start - RLS */
        COALESCE(ROW_SECU_ACCS_C,0) = 0 OR GETBIT( (
                SELECT
                        try_to_number(ROW_SECU_PRFL_C::varchar)  --Need to double check this logic
        FROM
                        NPD_D12_DMN_GDWMIG_IBRG_V.PVSECURITY.ROW_LEVL_SECU_USER_PRFL
        WHERE
                        UPPER(RTRIM( username)) = UPPER(RTRIM(CURRENT_USER))
       ),ROW_SECU_ACCS_C
       ) = 1
       /* End - RLS */
       );
 
/* <sc-view> PVSECURITY.ROW_LEVL_SECU_USER_PRFL </sc-view> */
---3.0 Create new 1:1 views for the above tables
CREATE OR REPLACE VIEW NPD_D12_DMN_GDWMIG_IBRG_V.PVSECURITY.ROW_LEVL_SECU_USER_PRFL
(
      USERNAME,
      ROW_SECU_PRFL_C,
      MY_SERVICE_NO,
      REQ_NO,
      RITM_NO,
      SAR_NO,
      CMMT,
      UPDT_USERNAME,
      UPDT_DATE,
      UPDT_DTTS
)
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": {  "major": 1,  "minor": 16,  "patch": "2.0" }, "attributes": {  "component": "teradata",  "convertedOn": "08/21/2025",  "domain": "snowflake" }}'
AS
SELECT
        USERNAME,
        ROW_SECU_PRFL_C,
        MY_SERVICE_NO,
        REQ_NO,
        RITM_NO,
        SAR_NO,
        CMMT,
        UPDT_USERNAME,
        UPDT_DATE,
        UPDT_DTTS
    FROM
        NPD_D12_DMN_GDWMIG_IBRG.PDSECURITY.ROW_LEVL_SECU_USER_PRFL
        ;


/* <sc-view> PVCBODS.ACCT_MSTR_CYT_DATA </sc-view> */
CREATE OR REPLACE VIEW NPD_D12_DMN_GDWMIG_IBRG_V.PVCBODS.ACCT_MSTR_CYT_DATA
(
    ACCT,
    BUSN_PTNR_NUMB,
    SRCE_SYST_ISAC_CODE,
    VALD_FROM,
    VALD_TO,
    USER_IN_CYT_CALC,
    CYT_DOCU_QOTE,
    RSDT_STUS,
    UPDT_MODE,
    LOAD_S,
    PROS_KEY_EFFT_I,
    ROW_SECU_ACCS_C,
    RECD_CNT,
    LAST_UPDT_ON,
    BDT_OBJC_APPT,
    CNCT_PART_ID,
    OBJC_FUNC,
    PROS_TIME_STMP,
    LAST_CHNG_BY,
    BUSN_TRAN_CATG_FOR_THE_CHNG,
    PDCG,
    CUST_ENHC_2_ID,
    TIME_STMP_SHRT_FORM,
    VALD_FROM_DTTS,
    VALD_TO_DTTS
)
COMMENT = '{ "origin": "sf_sc", "name": "snowconvert", "version": { "major": 1, "minor": 16, "patch": "2.0" }, "attributes": { "component": "teradata", "convertedOn": "08/21/2025", "domain": "snowflake" }}'
AS
SELECT
    ACCT,
    BUSN_PTNR_NUMB,
    SRCE_SYST_ISAC_CODE,
    VALD_FROM,
    VALD_TO,
    USER_IN_CYT_CALC,
    CYT_DOCU_QOTE,
    RSDT_STUS,
    UPDT_MODE,
    LOAD_S,
    PROS_KEY_EFFT_I,
    ROW_SECU_ACCS_C,
    RECD_CNT,
    LAST_UPDT_ON,
    BDT_OBJC_APPT,
    CNCT_PART_ID,
    OBJC_FUNC,
    PROS_TIME_STMP,
    LAST_CHNG_BY,
    BUSN_TRAN_CATG_FOR_THE_CHNG,
    PDCG,
    CUST_ENHC_2_ID,
    TIME_STMP_SHRT_FORM,
    VALD_FROM_DTTS,
    VALD_TO_DTTS
FROM
    NPD_D12_DMN_GDWMIG_IBRG.PDCBODS.ACCT_MSTR_CYT_DATA
WHERE
(
/* Start - RLS */
COALESCE(ROW_SECU_ACCS_C,0) = 0 OR GETBIT( (
SELECT
try_to_number(ROW_SECU_PRFL_C::varchar) --Need to double check this logic
FROM
NPD_D12_DMN_GDWMIG_IBRG_V.PVSECURITY.ROW_LEVL_SECU_USER_PRFL
WHERE
UPPER(RTRIM( username)) = UPPER(RTRIM(CURRENT_USER))
),ROW_SECU_ACCS_C
) = 1
/* End - RLS */
);



//////////////// tmp7 //////////////////////
CREATE OR REPLACE VIEW NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.ACCT_BALN
(
ACCT_I,
  BALN_TYPE_C,
  CALC_FUNC_C,
  TIME_PERD_C,
  EFFT_D,
  EXPY_D,
  BALN_A,
  CALC_F,
  PROS_KEY_EFFT_I,
  PROS_KEY_EXPY_I,
  EROR_SEQN_I,
  CNCY_C,
  SRCE_SYST_C,
  ROW_SECU_ACCS_C
) AS
SELECT
        ACCT_I,
  BALN_TYPE_C,
  CALC_FUNC_C,
  TIME_PERD_C,
  EFFT_D,
  EXPY_D,
  CASE WHEN (SRCE_SYST_C in ('SAP', 'WSS') AND (CNCY_C = 'AUD' OR CNCY_C IS NULL)) OR (COALESCE(SRCE_SYST_C,'') NOT in ('SAP', 'WSS'))
              THEN CAST(BALN_A AS DECIMAL (15,2)) 
              END AS BALN_A,
  CALC_F,
  PROS_KEY_EFFT_I,
  PROS_KEY_EXPY_I,
  EROR_SEQN_I,
  CNCY_C,
  SRCE_SYST_C,
  ROW_SECU_ACCS_C
  FROM
        NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.ACCT_BALN
 WHERE ( (SRCE_SYST_C in ('SAP', 'WSS') AND (CNCY_C = 'AUD' OR CNCY_C IS NULL)) 
  OR (COALESCE(SRCE_SYST_C,'') NOT in ('SAP', 'WSS') )  )
                                          
 AND ( 
(
/* Start - RLS */
COALESCE(ROW_SECU_ACCS_C,0) = 0 OR
GETBIT( (
SELECT
try_to_number(ROW_SECU_PRFL_C::varchar) --Need to double check this logic
FROM
NPD_D12_DMN_GDWMIG_IBRG_V.PVSECURITY.ROW_LEVL_SECU_USER_PRFL
WHERE
UPPER(RTRIM( username)) = UPPER(RTRIM(CURRENT_USER))
),ROW_SECU_ACCS_C
) =1
/* End - RLS */
)
    );



//////////////// tmp5 //////////////////////

-- Snowflake Views converted from Teradata
-- Generated automatically with dependency ordering
USE DATABASE NPD_D12_DMN_GDWMIG_IBRG_V;

-- drop SCHEMA IF EXISTS NPD_D12_DMN_GDWMIG_IBRG_V.D_D04_V_COX_001_STD_0;
-- drop SCHEMA IF EXISTS NPD_D12_DMN_GDWMIG_IBRG_V.JELIFFT;
-- -- drop SCHEMA IF EXISTS NPD_D12_DMN_GDWMIG_IBRG_V.PVDATA;
-- -- drop SCHEMA IF EXISTS NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH;
-- drop SCHEMA IF EXISTS NPD_D12_DMN_GDWMIG_IBRG_V.P_P01_PVTECH;
-- drop SCHEMA IF EXISTS NPD_D12_DMN_GDWMIG_IBRG_V.P_P01_V_COX_001_STD_0;
-- drop SCHEMA IF EXISTS NPD_D12_DMN_GDWMIG_IBRG_V.P_V_COX_001_STD_0;

-- ===============================
-- VIEWS IN DEPENDENCY ORDER
-- ===============================

--  1. PVTECH.DERV_PRTF_INT_HIST_PSST
CREATE OR REPLACE VIEW NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_INT_HIST_PSST AS
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
  NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_PRTF_INT_HIST_PSST
-- $LastChangedBy: jelifft $
-- $LastChangedDate: 2013-07-24 11:07:50 +1000 (Wed, 24 Jul 2013) $
-- $LastChangedRevision: 12357 $
;


-- STAR_CAD_PROD_DATA.DERV_PRTF_INT_PSST -> PVTECH.DERV_PRTF_INT_PSST

/* <sc-view> NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_INT_PSST </sc-view> */
/* Re-place the existing views */


--  2. PVTECH.DERV_PRTF_PATY_INT_GRUP_PSST
CREATE OR REPLACE VIEW NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_PATY_INT_GRUP_PSST AS
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
  NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_PRTF_PATY_INT_GRUP_PSST
-- $LastChangedBy: jelifft $
-- $LastChangedDate: 2013-07-24 11:07:50 +1000 (Wed, 24 Jul 2013) $
-- $LastChangedRevision: 12357 $
;


-- STAR_CAD_PROD_DATA.DERV_PRTF_PATY_OWN_REL -> PVTECH.DERV_PRTF_PATY_OWN_REL

/* <sc-view> NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_PATY_OWN_REL </sc-view> */



 

--  3. PVTECH.ACCT_BALN_BKDT_AUDT
CREATE OR REPLACE VIEW NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.ACCT_BALN_BKDT_AUDT
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
NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.ACCT_BALN_BKDT_AUDT;

-- NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.ACCT_PATY_TAX_INSS -> NPD_D12_DMN_GDWMIG_IBRG_V.P_P01_PVTECH.ACCT_PATY_TAX_INSS
/* <sc-view> NPD_D12_DMN_GDWMIG_IBRG_V.P_P01_PVTECH.ACCT_PATY_TAX_INSS </sc-view> */




--  4. PVTECH.ACCT_BALN_BKDT (depends on: PVSECURITY.ROW_LEVL_SECU_USER_PRFL)
CREATE OR REPLACE VIEW NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.ACCT_BALN_BKDT
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
        NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.ACCT_BALN_BKDT
 
 WHERE ( 
(
/* Start - RLS */
COALESCE(ROW_SECU_ACCS_C,0) = 0 OR
GETBIT( (
        SELECT
                    try_to_number(ROW_SECU_PRFL_C::varchar)  --Need to double check this logic
        FROM
                    NPD_D12_DMN_GDWMIG_IBRG_V.PVSECURITY.ROW_LEVL_SECU_USER_PRFL
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
CREATE OR REPLACE VIEW NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.ACCT_INT_GRUP
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
        NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.ACCT_INT_GRUP
 WHERE
(
/* Start - RLS */
COALESCE(ROW_SECU_ACCS_C,0) = 0 OR
GETBIT( (
        SELECT
                    try_to_number(ROW_SECU_PRFL_C::varchar)  --Need to double check this logic
        FROM
                    NPD_D12_DMN_GDWMIG_IBRG_V.PVSECURITY.ROW_LEVL_SECU_USER_PRFL
        WHERE
                    UPPER(RTRIM( username)) = UPPER(RTRIM(CURRENT_USER))
        ),ROW_SECU_ACCS_C
        ) = 1
/* End - RLS */
);


-- PVTECH.DERV_PRTF_ACCT_HIST_PSST -> PVTECH.DERV_PRTF_ACCT_HIST_PSST

/* <sc-view> NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_ACCT_HIST_PSST </sc-view> */


--  6. P_P01_PVTECH.ACCT_INT_GRUP
-- CREATE VIEW NPD_D12_DMN_GDWMIG_IBRG_V.P_P01_PVTECH.ACCT_INT_GRUP
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
--         NPD_D12_DMN_GDWMIG_IBRG.PP01STARCADPRODDATA.ACCT_INT_GRUP
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

/* <sc-view> NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.ACCT_INT_GRUP </sc-view> */



 

--  7. D_D04_V_COX_001_STD_0.ACCT_INT_GRUP
-- CREATE VIEW NPD_D12_DMN_GDWMIG_IBRG_V.D_D04_V_COX_001_STD_0.ACCT_INT_GRUP
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

-- NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.ACCT_INT_GRUP -> P_V_COX_001_STD_0.ACCT_INT_GRUP
/* <sc-view> P_V_COX_001_STD_0.ACCT_INT_GRUP </sc-view> */

--  8. PVTECH.GRD_PRTF_TYPE_ENHC_HIST_PSST
CREATE OR REPLACE VIEW NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.GRD_PRTF_TYPE_ENHC_HIST_PSST AS
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
  NPD_D12_DMN_GDWMIG_IBRG.PDGRD.GRD_PRTF_TYPE_ENHC_HIST_PSST
-- $LastChangedBy: jelifft $
-- $LastChangedDate: 2013-10-15 09:26:54 +1100 (Tue, 15 Oct 2013) $
-- $LastChangedRevision: 12741 $
;

--


-- PVTECH.GRD_PRTF_TYPE_ENHC_PSST -> PVTECH.GRD_PRTF_TYPE_ENHC_PSST

/* <sc-view> NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.GRD_PRTF_TYPE_ENHC_PSST </sc-view> */

-- --  9. P_P01_PVTECH.DERV_PRTF_ACCT_OWN_REL
-- CREATE  VIEW NPD_D12_DMN_GDWMIG_IBRG_V.P_P01_PVTECH.DERV_PRTF_ACCT_OWN_REL
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
--         NPD_D12_DMN_GDWMIG_IBRG.PP01STARCADPRODDATA.DERV_PRTF_ACCT_OWN_REL

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

/* <sc-view> NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_ACCT_OWN_REL </sc-view> */



 

-- 10. PVTECH.DERV_PRTF_INT_GRUP_OWN_PSST
CREATE OR REPLACE VIEW NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_INT_GRUP_OWN_PSST AS
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
  NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_PRTF_INT_GRUP_OWN_PSST
-- $LastChangedBy: jelifft $
-- $LastChangedDate: 2013-07-24 11:07:50 +1000 (Wed, 24 Jul 2013) $
-- $LastChangedRevision: 12357 $
;


-- STAR_CAD_PROD_DATA.DERV_PRTF_INT_HIST_PSST -> PVTECH.DERV_PRTF_INT_HIST_PSST

/* <sc-view> NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_INT_HIST_PSST </sc-view> */

-- 11. PVTECH.DERV_PRTF_PATY_OWN_REL (depends on: PVSECURITY.ROW_LEVL_SECU_USER_PRFL)
CREATE OR REPLACE VIEW NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_PATY_OWN_REL
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
        NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_PRTF_PATY_OWN_REL

 WHERE ( 
(
/* Start - RLS */
COALESCE(ROW_SECU_ACCS_C,0) = 0 OR
GETBIT( (
        SELECT
                    try_to_number(ROW_SECU_PRFL_C::varchar)  --Need to double check this logic
        FROM
                    NPD_D12_DMN_GDWMIG_IBRG_V.PVSECURITY.ROW_LEVL_SECU_USER_PRFL
        WHERE
                    UPPER(RTRIM( username)) = UPPER(RTRIM(CURRENT_USER))
        ),ROW_SECU_ACCS_C
        ) = 1
/* End - RLS */
)
    );




-- PVTECH.GRD_PRTF_TYPE_ENHC_HIST_PSST -> PVTECH.GRD_PRTF_TYPE_ENHC_HIST_PSST

/* <sc-view> NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.GRD_PRTF_TYPE_ENHC_HIST_PSST </sc-view> */
/* Create the new view */


-- 13. PVTECH.DERV_PRTF_INT_GRUP_ENHC_PSST
CREATE OR REPLACE VIEW NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_INT_GRUP_ENHC_PSST AS
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
  NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_PRTF_INT_GRUP_ENHC_PSST
-- $LastChangedBy: jelifft $
-- $LastChangedDate: 2013-07-24 11:07:50 +1000 (Wed, 24 Jul 2013) $
-- $LastChangedRevision: 12357 $
;


-- STAR_CAD_PROD_DATA.DERV_PRTF_INT_GRUP_OWN_PSST -> PVTECH.DERV_PRTF_INT_GRUP_OWN_PSST

/* <sc-view> NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_INT_GRUP_OWN_PSST </sc-view> */

-- 14. PVTECH.DERV_PRTF_ACCT_HIST_PSST
CREATE OR REPLACE VIEW NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_ACCT_HIST_PSST AS
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
  NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_PRTF_ACCT_HIST_PSST
-- $LastChangedBy: jelifft $
-- $LastChangedDate: 2013-07-24 11:07:50 +1000 (Wed, 24 Jul 2013) $
-- $LastChangedRevision: 12357 $
;


-- STAR_CAD_PROD_DATA.DERV_PRTF_ACCT_INT_GRUP_PSST -> PVTECH.DERV_PRTF_ACCT_INT_GRUP_PSST

/* <sc-view> NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_ACCT_INT_GRUP_PSST </sc-view> */

-- 15. PVTECH.DERV_PRTF_OWN_HIST_PSST
CREATE OR REPLACE VIEW NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_OWN_HIST_PSST AS
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
  NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_PRTF_OWN_HIST_PSST
-- $LastChangedBy: jelifft $
-- $LastChangedDate: 2013-07-22 10:21:37 +1000 (Mon, 22 Jul 2013) $
-- $LastChangedRevision: 12328 $
;


-- STAR_CAD_PROD_DATA.DERV_PRTF_OWN_PSST -> PVTECH.DERV_PRTF_OWN_PSST

/* <sc-view> NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_OWN_PSST </sc-view> */
/* Re-place the existing views */


-- -- 16. JELIFFT.GRD_PRTF_TYPE_ENHC (depends on: PVTECH.CALENDAR, PVTECH.DIMN_NODE_ASSC, PVTECH.GRD_PRTF_CATG_ATTR, PVTECH.GRD_PRTF_CLAS_ATTR, PVTECH.GRD_PRTF_TYPE_ATTR, PVTECH.MAP_SAP_INT_GRUP)
-- CREATE  VIEW NPD_D12_DMN_GDWMIG_IBRG_V.JELIFFT.GRD_PRTF_TYPE_ENHC as
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
--       NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.GRD_PRTF_TYPE_ATTR G
--       Inner Join NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.MAP_SAP_INT_GRUP MSIG
--       On MSIG.BUSN_PTNR_GRUP_TYPE = G.SAP_C
--       And G.CLAS_SCHM_C = 'PRTF_TYPE'

--       Inner Join NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.CALENDAR C
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
--       NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DIMN_NODE_ASSC D61 
--       Inner Join NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.CALENDAR C
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
--       NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.GRD_PRTF_CLAS_ATTR G
--       Inner Join NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.CALENDAR C
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
--       NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DIMN_NODE_ASSC D62 
--       Inner Join NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.CALENDAR C
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
--       NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.GRD_PRTF_CATG_ATTR G
--       Inner Join NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.CALENDAR C
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

/* <sc-view> NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.GRD_PRTF_TYPE_ENHC </sc-view> */

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
CREATE OR REPLACE VIEW NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_ACCT_PSST AS
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
  NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_PRTF_ACCT_PSST
-- $LastChangedBy: jelifft $
-- $LastChangedDate: 2013-10-22 16:53:07 +1100 (Tue, 22 Oct 2013) $
-- $LastChangedRevision: 12844 $
;


-- STAR_CAD_PROD_DATA.DERV_PRTF_INT_GRUP_ENHC_PSST -> PVTECH.DERV_PRTF_INT_GRUP_ENHC_PSST

/* <sc-view> NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_INT_GRUP_ENHC_PSST </sc-view> */

-- 18. PVTECH.GRD_PRTF_TYPE_ENHC_PSST
CREATE OR REPLACE VIEW NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.GRD_PRTF_TYPE_ENHC_PSST AS
SELECT
   PERD_D                        
  ,PRTF_TYPE_C                   
  ,PRTF_TYPE_M                   
  ,PRTF_CLAS_C                   
  ,PRTF_CLAS_M                   
  ,PRTF_CATG_C                   
  ,PRTF_CATG_M                   
FROM
  NPD_D12_DMN_GDWMIG_IBRG.PDGRD.GRD_PRTF_TYPE_ENHC_PSST
-- $LastChangedBy: jelifft $
-- $LastChangedDate: 2013-07-24 11:07:50 +1000 (Wed, 24 Jul 2013) $
-- $LastChangedRevision: 12357 $
;


-- STAR_CAD_PROD_DATA.ACCT_BALN_BKDT -> PVTECH.ACCT_BALN_BKDT

/* <sc-view> NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.ACCT_BALN_BKDT </sc-view> */



 

-- 19. P_P01_PVTECH.ACCT_PATY_TAX_INSS (depends on: P_P01_PVSECURITY.ROW_LEVL_SECU_USER_PRFL)
-- CREATE  VIEW NPD_D12_DMN_GDWMIG_IBRG_V.P_P01_PVTECH.ACCT_PATY_TAX_INSS
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
--         NPD_D12_DMN_GDWMIG_IBRG.PP01STARCADPRODDATA.ACCT_PATY_TAX_INSS ) ACCT_PATY_TAX_INSS
--  WHERE
-- (
-- /* Start - RLS */
-- COALESCE(ROW_SECU_ACCS_C,0) = 0 OR
-- GETBIT( (
--         SELECT
--                     try_to_number(ROW_SECU_PRFL_C::varchar)  --Need to double check this logic
--         FROM
--                     NPD_D12_DMN_GDWMIG_IBRG_V.P_P01_PVSECURITY.ROW_LEVL_SECU_USER_PRFL
--         WHERE
--                     UPPER(RTRIM( username)) = UPPER(RTRIM(CURRENT_USER))
--         ),ROW_SECU_ACCS_C
--         ) = 1
-- /* End - RLS */
-- );

-- NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.ACCT_PATY_TAX_INSS -> P_V_COX_001_STD_0.ACCT_PATY_TAX_INSS
/* <sc-view> P_V_COX_001_STD_0.ACCT_PATY_TAX_INSS </sc-view> */

-- 20. PVTECH.ACCT_PATY_TAX_INSS (depends on: PVSECURITY.ROW_LEVL_SECU_USER_PRFL)
-- CREATE  VIEW NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.ACCT_PATY_TAX_INSS
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
--         NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.ACCT_PATY_TAX_INSS ) ACCT_PATY_TAX_INSS
--  WHERE
-- (
-- /* Start - RLS */
-- COALESCE(ROW_SECU_ACCS_C,0) = 0 OR
-- GETBIT( (
--         SELECT
--                     try_to_number(ROW_SECU_PRFL_C::varchar)  --Need to double check this logic
--         FROM
--                     NPD_D12_DMN_GDWMIG_IBRG_V.PVSECURITY.ROW_LEVL_SECU_USER_PRFL
--         WHERE
--                     UPPER(RTRIM( username)) = UPPER(RTRIM(CURRENT_USER))
--         ),ROW_SECU_ACCS_C
--         ) = 1
-- /* End - RLS */
-- );


-- STAR_CAD_PROD_DATA.DERV_PRTF_ACCT_HIST_PSST -> PVTECH.DERV_PRTF_ACCT_HIST_PSST

/* <sc-view> NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_ACCT_HIST_PSST </sc-view> */

-- 21. PVTECH.DERV_PRTF_INT_PSST
CREATE OR REPLACE VIEW NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_INT_PSST AS
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
  NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_PRTF_INT_PSST
-- $LastChangedBy: jelifft $
-- $LastChangedDate: 2013-10-21 16:39:07 +1100 (Mon, 21 Oct 2013) $
-- $LastChangedRevision: 12832 $
;


-- STAR_CAD_PROD_DATA.DERV_PRTF_OWN_HIST_PSST -> PVTECH.DERV_PRTF_OWN_HIST_PSST

/* <sc-view> NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_OWN_HIST_PSST </sc-view> */
/* Re-place the existing views */


-- 22. PVTECH.DERV_PRTF_ACCT_OWN_REL (depends on: PVSECURITY.ROW_LEVL_SECU_USER_PRFL)
CREATE OR REPLACE VIEW NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_ACCT_OWN_REL
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
        NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_PRTF_ACCT_OWN_REL

 WHERE ( 
(
/* Start - RLS */
COALESCE(ROW_SECU_ACCS_C,0) = 0 OR
GETBIT( (
        SELECT
                    try_to_number(ROW_SECU_PRFL_C::varchar)  --Need to double check this logic
        FROM
                    NPD_D12_DMN_GDWMIG_IBRG_V.PVSECURITY.ROW_LEVL_SECU_USER_PRFL
        WHERE
                    UPPER(RTRIM( username)) = UPPER(RTRIM(CURRENT_USER))
        ),ROW_SECU_ACCS_C
        ) = 1
/* End - RLS */
)
    );


-- STAR_CAD_PROD_DATA.DERV_PRTF_ACCT_PSST -> PVTECH.DERV_PRTF_ACCT_PSST

/* <sc-view> NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_ACCT_PSST </sc-view> */
/* Re-place the existing views */


-- 23. PVTECH.DERV_PRTF_OWN_PSST
CREATE OR REPLACE VIEW NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_OWN_PSST AS
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
  NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_PRTF_OWN_PSST
-- $LastChangedBy: jelifft $
-- $LastChangedDate: 2013-10-23 09:10:24 +1100 (Wed, 23 Oct 2013) $
-- $LastChangedRevision: 12847 $
;


-- STAR_CAD_PROD_DATA.DERV_PRTF_PATY_HIST_PSST -> PVTECH.DERV_PRTF_PATY_HIST_PSST

/* <sc-view> NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_PATY_HIST_PSST </sc-view> */
/* Re-place the existing views */


-- 24. PVTECH.DERV_PRTF_ACCT_INT_GRUP_PSST
CREATE OR REPLACE VIEW NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_ACCT_INT_GRUP_PSST AS
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
  NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_PRTF_ACCT_INT_GRUP_PSST
-- $LastChangedBy: jelifft $
-- $LastChangedDate: 2013-07-24 11:07:50 +1000 (Wed, 24 Jul 2013) $
-- $LastChangedRevision: 12357 $
;

-- NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_PRTF_ACCT_OWN_REL -> NPD_D12_DMN_GDWMIG_IBRG_V.P_P01_PVTECH.DERV_PRTF_ACCT_OWN_REL
/* <sc-view> NPD_D12_DMN_GDWMIG_IBRG_V.P_P01_PVTECH.DERV_PRTF_ACCT_OWN_REL </sc-view> */







-- 25. PVTECH.DERV_PRTF_PATY_HIST_PSST
CREATE OR REPLACE VIEW NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_PATY_HIST_PSST AS
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
  NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_PRTF_PATY_HIST_PSST
-- $LastChangedBy: jelifft $
-- $LastChangedDate: 2013-07-22 10:21:37 +1000 (Mon, 22 Jul 2013) $
-- $LastChangedRevision: 12328 $
;


-- STAR_CAD_PROD_DATA.DERV_PRTF_PATY_INT_GRUP_PSST -> PVTECH.DERV_PRTF_PATY_INT_GRUP_PSST

/* <sc-view> NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DERV_PRTF_PATY_INT_GRUP_PSST </sc-view> */

-- 26. PVDATA.ACCT_BALN_BKDT_AUDT (depends on: PVTECH.ACCT_BALN_BKDT_AUDT)
CREATE OR REPLACE VIEW NPD_D12_DMN_GDWMIG_IBRG_V.PVDATA.ACCT_BALN_BKDT_AUDT
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
NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.ACCT_BALN_BKDT_AUDT;


-- STAR_CAD_PROD_DATA.ACCT_BALN_BKDT_AUDT -> PVTECH.ACCT_BALN_BKDT_AUDT

/* <sc-view> NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.ACCT_BALN_BKDT_AUDT </sc-view> */

-- 27. P_V_COX_001_STD_0.ACCT_INT_GRUP (depends on: PVTECH.ACCT_INT_GRUP)
-- CREATE VIEW NPD_D12_DMN_GDWMIG_IBRG_V.P_V_COX_001_STD_0.ACCT_INT_GRUP
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
-- NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.ACCT_INT_GRUP
-- ;

-- NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.ACCT_INT_GRUP -> P_P01_V_COX_001_STD_0.ACCT_INT_GRUP
/* <sc-view> P_P01_V_COX_001_STD_0.ACCT_INT_GRUP </sc-view> */







-- 28. P_P01_V_COX_001_STD_0.ACCT_INT_GRUP (depends on: P_P01_PVTECH.ACCT_INT_GRUP)
-- CREATE VIEW NPD_D12_DMN_GDWMIG_IBRG_V.P_P01_V_COX_001_STD_0.ACCT_INT_GRUP
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
-- NPD_D12_DMN_GDWMIG_IBRG_V.P_P01_PVTECH.ACCT_INT_GRUP;

-- NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.ACCT_INT_GRUP -> NPD_D12_DMN_GDWMIG_IBRG_V.P_P01_PVTECH.ACCT_INT_GRUP
/* <sc-view> NPD_D12_DMN_GDWMIG_IBRG_V.P_P01_PVTECH.ACCT_INT_GRUP </sc-view> */




-- 29. P_P01_V_COX_001_STD_0.ACCT_PATY_TAX_INSS (depends on: P_P01_PVTECH.ACCT_PATY_TAX_INSS)
-- CREATE VIEW NPD_D12_DMN_GDWMIG_IBRG_V.P_P01_V_COX_001_STD_0.ACCT_PATY_TAX_INSS
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
--         NPD_D12_DMN_GDWMIG_IBRG_V.P_P01_PVTECH.ACCT_PATY_TAX_INSS;


-- STAR_CAD_PROD_DATA.ACCT_PATY_TAX_INSS -> PVTECH.ACCT_PATY_TAX_INSS

/* <sc-view> NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.ACCT_PATY_TAX_INSS </sc-view> */



-- 30. P_V_COX_001_STD_0.ACCT_PATY_TAX_INSS (depends on: PVTECH.ACCT_PATY_TAX_INSS)
-- CREATE VIEW NPD_D12_DMN_GDWMIG_IBRG_V.P_V_COX_001_STD_0.ACCT_PATY_TAX_INSS
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
--         NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.ACCT_PATY_TAX_INSS;

-- NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.ACCT_PATY_TAX_INSS -> D_D04_V_COX_001_STD_0.ACCT_PATY_TAX_INSS
/* <sc-view> D_D04_V_COX_001_STD_0.ACCT_PATY_TAX_INSS </sc-view> */


-- 31. D_D04_V_COX_001_STD_0.ACCT_PATY_TAX_INSS (depends on: PVTECH.ACCT_PATY_TAX_INSS)
-- CREATE VIEW NPD_D12_DMN_GDWMIG_IBRG_V.D_D04_V_COX_001_STD_0.ACCT_PATY_TAX_INSS
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
--         NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.ACCT_PATY_TAX_INSS;

-- NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.ACCT_PATY_TAX_INSS -> P_P01_V_COX_001_STD_0.ACCT_PATY_TAX_INSS
/* <sc-view> P_P01_V_COX_001_STD_0.ACCT_PATY_TAX_INSS </sc-view> */







-- 12. PVTECH.GRD_PRTF_TYPE_ENHC (depends on: PVTECH.CALENDAR, PVTECH.DIMN_NODE_ASSC, PVTECH.GRD_PRTF_CATG_ATTR, PVTECH.GRD_PRTF_CLAS_ATTR, PVTECH.GRD_PRTF_TYPE_ATTR, PVTECH.MAP_SAP_INT_GRUP, PVTECH.TYPE_INT_GRUP)
-- CREATE VIEW NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.GRD_PRTF_TYPE_ENHC as
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
--        MSIG.INT_GRUP_TYPE_C as PRTF_TYPE_C
--       ,TIG.INT_GRUP_TYPE_M as PRTF_TYPE_M
--       ,G.CLAS_SCHM_C
--       ,G.PRTF_TYPE_NODE_C
--       ,G.EFFT_D
--       ,C.CALENDAR_DATE as PERD_D
--     From
--       NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.GRD_PRTF_TYPE_ATTR G
--       Inner Join NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.MAP_SAP_INT_GRUP MSIG
--       On MSIG.BUSN_PTNR_GRUP_TYPE = G.SAP_C
--       And G.CLAS_SCHM_C = 'PRTF_TYPE'

--       Inner Join NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.TYPE_INT_GRUP TIG
--       On TIG.INT_GRUP_TYPE_C = MSIG.INT_GRUP_TYPE_C

--       Inner Join NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.CALENDAR C
--       On C.CALENDAR_DATE between G.NODE_EFFT_D and G.NODE_EXPY_D
--       And C.CALENDAR_DATE between MSIG.EFFT_D and MSIG.EXPY_D
--       And C.CALENDAR_DATE between TIG.EFFT_D and TIG.EXPY_D
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
--       NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DIMN_NODE_ASSC D61 
--       Inner Join NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.CALENDAR C
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
--       NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.GRD_PRTF_CLAS_ATTR G
--       Inner Join NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.CALENDAR C
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
--       NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.DIMN_NODE_ASSC D62 
--       Inner Join NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.CALENDAR C
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
--       NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.GRD_PRTF_CATG_ATTR G
--       Inner Join NPD_D12_DMN_GDWMIG_IBRG_V.PVTECH.CALENDAR C
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

-- -- $LastChangedBy: jelifft $
-- -- $LastChangedDate: 2013-07-17 12:11:03 +1000 (Wed, 17 Jul 2013) $
-- -- $LastChangedRevision: 12304 $
-- ;
