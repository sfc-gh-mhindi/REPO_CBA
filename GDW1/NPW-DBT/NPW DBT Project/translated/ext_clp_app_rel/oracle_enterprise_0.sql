{{ config(materialized='view', tags=['EXT_CLP_APP_REL']) }}

WITH 
clp_app_prod AS (
	SELECT
	*
	FROM {{ source("pin_owner","clp_app_prod")  }}),
app_prod AS (
	SELECT
	*
	FROM {{ source("pcd_owner","app_prod")  }}),
Oracle_Enterprise_0 AS (/* Formatted on 2008/03/03 16:37 (Formatter Plus v4.8.7) */ SELECT "RPT_ROW" FROM (SELECT 'D|' || CAST(MOD_TIMESTAMP AS TEXT) || '|' || APP_ID || '|' || SUBTYPE_CODE || '|' || LOAN_APP_ID || '|' || LOAN_SUBTYPE_CODE || '|' AS RPT_ROW FROM (SELECT CAP.MOD_TIMESTAMP, a1.APP_ID AS APP_ID, a1.SUBTYPE_CODE AS SUBTYPE_CODE, a.APP_ID AS LOAN_APP_ID, a.SUBTYPE_CODE AS LOAN_SUBTYPE_CODE FROM app_prod, app_prod, clp_app_prod WHERE a.APP_PROD_ID = cap.LOAN_APP_PROD_ID AND a1.APP_PROD_ID = cap.clp_APP_PROD_ID AND CAP.MOD_TIMESTAMP BETWEEN STR_TO_DATE('{{ var("FBIZDATE") }}', '%Y%m%d%H%M%S') AND STR_TO_DATE('{{ var("TBIZDATE") }}', '%Y%m%d%H%M%S'))))


SELECT * FROM Oracle_Enterprise_0