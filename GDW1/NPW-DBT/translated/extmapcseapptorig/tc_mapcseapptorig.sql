{{ config(materialized='view', tags=['ExtMapCseApptOrig']) }}

WITH 
,
tc_MapCseApptorig AS (SELECT CHNL_CAT_ID, APPT_ORIG_C, EFFT_D,EXPY_D FROM {{ var('pGDW_TECH_DB') }}.{{ var('pcMAP_TABLE_ORIG') }} WHERE CAST('{{ var('pRUN_STRM_PROS_D') }}' AS DATE FORMAT 'YYYYMMDD') BETWEEN EFFT_D(DATE,FORMAT 'YYYYMMDD')  AND EXPY_D(DATE,FORMAT 'YYYYMMDD');)


SELECT * FROM tc_MapCseApptorig