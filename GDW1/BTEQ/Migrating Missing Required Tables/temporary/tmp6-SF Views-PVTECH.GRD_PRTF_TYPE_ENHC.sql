-- Snowflake Views converted from Teradata
-- Generated automatically

USE DATABASE PS_GDW1_BTEQ;

-- DDLs for PS_GDW1_BTEQ.PVTECH.GRD_PRTF_TYPE_ENHC view
-- Extracted from CBA_GDW_DDL_Extracts

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

