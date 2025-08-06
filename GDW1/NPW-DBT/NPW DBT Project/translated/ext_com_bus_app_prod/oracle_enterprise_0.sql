{{ config(materialized='view', tags=['EXT_COM_BUS_APP_PROD']) }}

WITH 
app_prod AS (
	SELECT
	*
	FROM {{ source("pcd_owner","app_prod")  }}),
grd_product_type_map AS (
	SELECT
	*
	FROM {{ source("gssr_extract","grd_product_type_map")  }}),
Oracle_Enterprise_0 AS (/* Formatted on 2008/03/03 16:37 (Formatter Plus v4.8.7) */ SELECT RPT_ROW FROM (SELECT 'D|' || CAST(MOD_TIMESTAMP AS TEXT) || '|' || APP_PROD_ID || '|' || SUBTYPE_CODE || '|' || APP_ID || '|' || PDCT_N || '|' || SM_CASE_ID || '|' AS RPT_ROW FROM (SELECT AP.MOD_TIMESTAMP, AP.APP_PROD_ID, AP.SUBTYPE_CODE, AP.APP_ID, GPC.GRD_PDCT_N AS PDCT_N, AP.SM_CASE_ID FROM APP_PROD, GRD_PRODUCT_TYPE_MAP WHERE AP.PRODUCT_TYPE_ID = GPC.PRODUCT_TYPE_ID() AND AP.SUBTYPE_CODE IN ('CLP') AND AP.mod_timestamp BETWEEN STR_TO_DATE('{{ var("FBIZDATE") }}', '%Y%m%d%H%M%S') AND STR_TO_DATE('{{ var("TBIZDATE") }}', '%Y%m%d%H%M%S'))))


SELECT * FROM Oracle_Enterprise_0