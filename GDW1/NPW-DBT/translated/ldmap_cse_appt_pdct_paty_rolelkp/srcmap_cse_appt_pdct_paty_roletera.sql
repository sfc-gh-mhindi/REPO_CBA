{{ config(materialized='view', tags=['LdMAP_CSE_APPT_PDCT_PATY_ROLELkp']) }}

WITH 
,
SrcMAP_CSE_APPT_PDCT_PATY_ROLETera AS (SELECT ROLE_CAT_ID,PATY_ROLE_C,EFFT_D,EXPY_D FROM {{ var('GDW_ACCT_VW') }}.MAP_CSE_APPT_PDCT_PATY_ROLE WHERE efft_d (date, format 'yyyymmdd') <= '{{ var('ETL_PROCESS_DT') }}' and expy_d (date, format 'yyyymmdd') >= '{{ var('ETL_PROCESS_DT') }}';)


SELECT * FROM SrcMAP_CSE_APPT_PDCT_PATY_ROLETera