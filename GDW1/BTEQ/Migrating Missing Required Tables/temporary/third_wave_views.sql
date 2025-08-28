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
        PDSECURITY.ROW_LEVL_SECU_USER_PRFL
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