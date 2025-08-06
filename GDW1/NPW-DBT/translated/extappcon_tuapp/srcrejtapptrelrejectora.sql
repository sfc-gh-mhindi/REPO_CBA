{{ config(materialized='view', tags=['ExtAppCon_TuApp']) }}

WITH 
rejt_chl_bus_tu_app_cond AS (
	SELECT
	*
	FROM {{ source("cse2_stg","rejt_chl_bus_tu_app_cond")  }}),
SrcRejtApptRelRejectOra AS (/* Formatted on 2007/10/03 11:28 (Formatter Plus v4.8.7) */ SELECT tu_app_condition_id, tu_app_condition_cat_id, CAST(condition_met_date AS TEXT) AS condition_met_date, subtype_code, hl_app_prod_id, etl_d, CAST(orig_etl_d AS TEXT) AS orig_etl_d, eror_c FROM REJT_CHL_BUS_TU_APP_COND WHERE eror_c LIKE 'RPR%')


SELECT * FROM SrcRejtApptRelRejectOra