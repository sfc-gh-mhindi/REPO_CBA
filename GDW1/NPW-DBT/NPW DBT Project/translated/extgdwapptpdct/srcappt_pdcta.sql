{{ config(materialized='view', tags=['ExtGdwApptPdct']) }}

WITH 
,
SrcAPPT_PDCTA AS (select
      A.APPT_PDCT_I as APPT_PDCT_I,
     B.APPT_PDCT_I AS RELD_APPT_PDCT_I,
     A.EXPY_FLAG
from
(
SELECT 
     APPT_PDCT_I,
      APPT_I,
      EFFT_D,
      'N' as EXPY_FLAG
FROM {{ var('GDW_ACCT_VW') }}.APPT_PDCT
WHERE APPT_QLFY_C IN ('HP','PP') 
AND  EFFT_D (date, format 'yyyymmdd') = '{{ var('ETL_PROCESS_DT') }}'
AND EXPY_D (date, format 'yyyymmdd') = (cast('99991231'  as date format 'yyyymmdd') )
UNION
SELECT 
     APPT_PDCT_I,
      APPT_I,
      EFFT_D,
      'Y' as EXPY_FLAG
FROM {{ var('GDW_ACCT_VW') }}.APPT_PDCT
WHERE APPT_QLFY_C IN ('HP','PP') 
AND EXPY_D (date, format 'yyyymmdd') = (cast('{{ var('ETL_PROCESS_DT') }}'  as date format 'yyyymmdd')-1 )
) as A,
(
SELECT 
     APPT_PDCT_I,
      APPT_I
FROM {{ var('GDW_ACCT_VW') }}.APPT_PDCT
WHERE APPT_QLFY_C IN ('HL','PL') AND EXPY_D (date, format 'yyyymmdd') = '99991231'
) as B
WHERE A.APPT_I = B.APPT_I
)


SELECT * FROM SrcAPPT_PDCTA