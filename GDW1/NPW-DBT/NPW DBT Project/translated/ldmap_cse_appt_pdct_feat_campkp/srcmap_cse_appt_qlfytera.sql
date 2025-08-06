{{ config(materialized='view', tags=['LdMAP_CSE_APPT_PDCT_FEAT_CAMPkp']) }}

WITH 
,
SrcMAP_CSE_APPT_QLFYTera AS (SELECT CAMPAIGN_CAT_ID,FEAT_I,ACTL_VALU_R,EFFT_D,EXPY_D FROM {{ var('GDW_ACCT_VW') }}.MAP_CSE_CPGN_CATG_FEAT WHERE efft_d (date, format 'yyyymmdd') <= '{{ var('ETL_PROCESS_DT') }}' and expy_d (date, format 'yyyymmdd') >= '{{ var('ETL_PROCESS_DT') }}';)


SELECT * FROM SrcMAP_CSE_APPT_QLFYTera