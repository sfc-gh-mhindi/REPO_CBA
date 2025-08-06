{{ config(materialized='view', tags=['EXT_CPL_BUS_PL_APP_ANS']) }}

WITH 
pl_app_prod_client_role AS (
	SELECT
	*
	FROM {{ source("pcl_owner","pl_app_prod_client_role")  }}),
app AS (
	SELECT
	*
	FROM {{ source("pcd_owner","app")  }}),
app_prod_client_role AS (
	SELECT
	*
	FROM {{ source("pcd_owner","app_prod_client_role")  }}),
pl_app_answer AS (
	SELECT
	*
	FROM {{ source("pcl_owner","pl_app_answer")  }}),
organisation_unit AS (
	SELECT
	*
	FROM {{ source("pfs_owner","organisation_unit")  }}),
app_prod AS (
	SELECT
	*
	FROM {{ source("pcd_owner","app_prod")  }}),
client AS (
	SELECT
	*
	FROM {{ source("pfs_owner","client")  }}),
Oracle_Enterprise_0 AS (SELECT RPT_ROW FROM (SELECT 'D|' || CAST(MOD_TIMESTAMP AS TEXT) || '|' || PL_APP_ID || '|' || QA_QUESTION_ID || '|' || QA_ANSWER_ID || '|' || TEXT_ANSWER || '|' || CIF_CODE || '|' || CBA_STAFF_NUMBER || '|' || GL_DEPT_NUMBER || '|' || SUBTYPE_CODE || '|' AS RPT_ROW FROM (SELECT PA.MOD_TIMESTAMP, PA.PL_APP_ID, PA.QA_QUESTION_ID, PA.QA_ANSWER_ID, TRANSLATE(PA.TEXT_ANSWER, CHR(10) || CHR(13) || '|', '   ') AS TEXT_ANSWER, C.CIF_CODE, OU.CBA_STAFF_NUMBER, (SELECT OU2.GL_DEPT_NUMBER FROM organisation_unit WHERE a.LODGEMENT_BRANCH_ID = OU2.CS_PREMIUM_CENTRE_ID) AS GL_DEPT_NUMBER, a.SUBTYPE_CODE FROM app_prod_client_role, pl_app_prod_client_role, CLIENT, app_prod, app, organisation_unit, Pl_APP_ANSWER WHERE apc.APP_PROD_CLIENT_ROLE_ID = pla.PL_APP_PROD_CLIENT_ROLE_ID AND apc.CLIENT_ID = c.CLIENT_ID AND ap.APP_PROD_ID = apc.APP_PROD_ID AND ap.APP_ID = a.APP_ID AND PA.MOD_USER_ID = ou.CS_USER_ID AND PA.PL_APP_ID = a.APP_ID AND PA.mod_timestamp BETWEEN STR_TO_DATE('{{ var("FBIZDATE") }}', '%Y%m%d%H%M%S') AND STR_TO_DATE('{{ var("TBIZDATE") }}', '%Y%m%d%H%M%S'))))


SELECT * FROM Oracle_Enterprise_0