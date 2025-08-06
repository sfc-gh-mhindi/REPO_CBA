{{ config(materialized='view', tags=['XfmChlBusHlmAppSecFrmExt']) }}

WITH 
,
SrcMapApptAsetStloHm AS (SELECT FRWD_DOCU_TO,FRWD_DOCU_C FROM {{ var('GDW_ACCT_VW') }}.MAP_CSE_APPT_ASET_STLO_HM WHERE efft_d (date, format 'yyyymmdd') <= '{{ var('ETL_PROCESS_DT') }}' and expy_d (date, format 'yyyymmdd') >= '{{ var('ETL_PROCESS_DT') }}' )


SELECT * FROM SrcMapApptAsetStloHm