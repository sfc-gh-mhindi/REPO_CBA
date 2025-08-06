{{ config(materialized='view', tags=['EXT_COM_BUS_SM_CASE_STATE']) }}

WITH 
organisation_unit AS (
	SELECT
	*
	FROM {{ source("pfs_owner","organisation_unit")  }}),
app AS (
	SELECT
	*
	FROM {{ source("pcd_owner","app")  }}),
sm_case_state AS (
	SELECT
	*
	FROM {{ source("pcd_owner","sm_case_state")  }}),
Oracle_Enterprise_0 AS (SELECT "RPT_ROW" FROM (SELECT 'D|' || CAST(MOD_TIMESTAMP AS TEXT) || '|' || SM_CASE_STATE_ID || '|' || SM_CASE_ID || '|' || SM_STATE_CAT_ID || '|' || CAST(START_DATE AS TEXT) || '|' || CAST(END_DATE AS TEXT) || '|' || CREATED_BY_STAFF_NUMBER || '|' || STATE_CAUSED_BY_ACTION_ID || '|' AS RPT_ROW FROM (SELECT SM.MOD_TIMESTAMP, SM.SM_CASE_STATE_ID, SM.SM_CASE_ID, SM.SM_STATE_CAT_ID, SM.START_DATE, SM.END_DATE, OU.CBA_STAFF_NUMBER AS CREATED_BY_STAFF_NUMBER, SM.STATE_CAUSED_BY_ACTION_ID FROM APP, SM_CASE_STATE, ORGANISATION_UNIT WHERE a.SM_CASE_ID = SM.SM_CASE_ID AND A.SUBTYPE_CODE = 'CLP' AND SM.CREATED_BY_USER_ID = OU.CS_USER_ID AND SM.mod_timestamp BETWEEN STR_TO_DATE('{{ var("FBIZDATE") }}', '%Y%m%d%H%M%S') AND STR_TO_DATE('{{ var("TBIZDATE") }}', '%Y%m%d%H%M%S'))))


SELECT * FROM Oracle_Enterprise_0