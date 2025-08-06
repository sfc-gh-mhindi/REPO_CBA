{{ config(materialized='view', tags=['EXT_CLP_BUS_APP_PROD_ANS']) }}

WITH 
clp_app_prod_answer AS (
	SELECT
	*
	FROM {{ source("pin_owner","clp_app_prod_answer")  }}),
app_prod AS (
	SELECT
	*
	FROM {{ source("pcd_owner","app_prod")  }}),
app AS (
	SELECT
	*
	FROM {{ source("pcd_owner","app")  }}),
Oracle_Enterprise_0 AS (SELECT RPT_ROW FROM (SELECT 'D|' || CAST(MOD_TIMESTAMP AS TEXT) || '|' || APP_ID || '|' || APP_PROD_ID || '|' || SUBTYPE_CODE || '|' || CLP_APP_PROD_ANSWER_VN || '|' || CLP_APP_PROD_ANSWER_ID || '|' || QA_QUESTION_ID || '|' || QA_ANSWER_ID || '|' || TEXT_ANSWER || '|' || YN_FLAG_ANSWER || '|' || NUMERIC_ANSWER || '|' || CAST(DATE_ANSWER AS TEXT) || '|' || MOD_USER_ID || '|' AS RPT_ROW FROM (SELECT CAPA.MOD_TIMESTAMP, A.APP_ID, AP.APP_PROD_ID, A.SUBTYPE_CODE, CAPA.CLP_APP_PROD_ANSWER_VN, CAPA.CLP_APP_PROD_ANSWER_ID, CAPA.QA_QUESTION_ID, CAPA.QA_ANSWER_ID, TRANSLATE(CAPA.TEXT_ANSWER, CHR(10) || CHR(13) || '|', '   ') AS TEXT_ANSWER, CAPA.YN_FLAG_ANSWER, CAPA.NUMERIC_ANSWER, CAPA.DATE_ANSWER, CAPA.MOD_USER_ID FROM app, app_prod, clp_app_prod_answer WHERE a.APP_ID = ap.APP_ID AND ap.APP_PROD_ID = capa.CLP_APP_PROD_ID AND CAPA.mod_timestamp BETWEEN STR_TO_DATE('{{ var("FBIZDATE") }}', '%Y%m%d%H%M%S') AND STR_TO_DATE('{{ var("TBIZDATE") }}', '%Y%m%d%H%M%S'))))


SELECT * FROM Oracle_Enterprise_0