{{ config(materialized='view', tags=['ExtMapCseApptQlfy']) }}

WITH 
,
tc_MapCseApptQlfy AS (SELECT SBTY_CODE, APPT_QLFY_C, EFFT_D, EXPY_D FROM {{ var('pGDW_TECH_DB') }}.{{ var('pcMAP_TABLE_QLFY') }} WHERE CAST('{{ var('pRUN_STRM_PROS_D') }}' AS DATE FORMAT 'YYYYMMDD') BETWEEN EFFT_D(DATE,FORMAT 'YYYYMMDD')  AND EXPY_D(DATE,FORMAT 'YYYYMMDD');)


SELECT * FROM tc_MapCseApptQlfy