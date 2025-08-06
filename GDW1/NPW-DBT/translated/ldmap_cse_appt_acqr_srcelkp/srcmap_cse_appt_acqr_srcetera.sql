{{ config(materialized='view', tags=['LdMAP_CSE_APPT_ACQR_SRCELkp']) }}

WITH 
,
SrcMAP_CSE_APPT_ACQR_SRCETera AS (SELECT PL_MRKT_SRCE_CAT_ID,ACQR_SRCE_C FROM {{ var('GDW_ACCT_VW') }}.MAP_CSE_APPT_ACQR_SRCE WHERE efft_d (date, format 'yyyymmdd') <= '{{ var('ETL_PROCESS_DT') }}' and expy_d (date, format 'yyyymmdd') >= '{{ var('ETL_PROCESS_DT') }}';)


SELECT * FROM SrcMAP_CSE_APPT_ACQR_SRCETera