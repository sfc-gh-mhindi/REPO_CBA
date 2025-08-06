{{ config(materialized='view', tags=['EXT_CHL_BUS_HL_APP_ANS']) }}

WITH 
hl_app_answer AS (
	SELECT
	*
	FROM {{ source("chl_owner","hl_app_answer")  }}),
app AS (
	SELECT
	*
	FROM {{ source("pcd_owner","app")  }}),
hl_app_client AS (
	SELECT
	*
	FROM {{ source("chl_owner","hl_app_client")  }}),
organisation_unit AS (
	SELECT
	*
	FROM {{ source("pfs_owner","organisation_unit")  }}),
client AS (
	SELECT
	*
	FROM {{ source("pfs_owner","client")  }}),
Oracle_Enterprise_0 AS (/* Formatted on 2008/03/05 11:30 (Formatter Plus v4.8.7) */ SELECT RPT_ROW FROM (SELECT 'D|' || CAST(MOD_TIMESTAMP AS TEXT) || '|' || HL_APP_ID || '|' || QA_QUESTION_ID || '|' || QA_ANSWER_ID || '|' || TEXT_ANSWER || '|' || CIF_CODE || '|' || CBA_STAFF_NUMBER || '|' || GL_DEPT_NUMBER || '|' || SUBTYPE_CODE || '|' AS RPT_ROW FROM (SELECT HA.MOD_TIMESTAMP, HA.HL_APP_ID, HA.QA_QUESTION_ID, HA.QA_ANSWER_ID, TRANSLATE(HA.TEXT_ANSWER, CHR(10) || CHR(13) || '|', '   ') AS TEXT_ANSWER, C.CIF_CODE, OU.CBA_STAFF_NUMBER, (SELECT OU2.GL_DEPT_NUMBER FROM organisation_unit WHERE a.LODGEMENT_BRANCH_ID = OU2.CS_PREMIUM_CENTRE_ID) AS GL_DEPT_NUMBER, A.SUBTYPE_CODE FROM hl_app_client, CLIENT, organisation_unit, hl_app_answer, app WHERE c.CLIENT_ID = hac.CLIENT_ID AND hac.HL_APP_ID = ha.HL_APP_ID AND HA.MOD_USER_ID = ou.CS_USER_ID AND HA.HL_APP_ID = a.APP_ID AND HA.mod_timestamp BETWEEN STR_TO_DATE('{{ var("FBIZDATE") }}', '%Y%m%d%H%M%S') AND STR_TO_DATE('{{ var("TBIZDATE") }}', '%Y%m%d%H%M%S'))))


SELECT * FROM Oracle_Enterprise_0