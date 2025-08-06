{{ config(materialized='view', tags=['ExtCplApptRel']) }}

WITH 
rejt_clp_bus_app_prod_appt_rel AS (
	SELECT
	*
	FROM {{ source("cse4_stg","rejt_clp_bus_app_prod_appt_rel")  }}),
SrcRejtCCAppProdBalXferRejectOra AS (SELECT APPT_I, RELD_APPT_I, REL_TYPE_C, LOAN_SUBTYPE_CODE, CAST(ORIG_ETL_D AS TEXT) AS ORIG_ETL_D FROM rejt_clp_bus_app_prod_appt_rel WHERE eror_c LIKE 'RPR%')


SELECT * FROM SrcRejtCCAppProdBalXferRejectOra