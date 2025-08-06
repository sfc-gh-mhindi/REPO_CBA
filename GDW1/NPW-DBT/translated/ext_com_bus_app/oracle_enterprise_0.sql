{{ config(materialized='view', tags=['EXT_COM_BUS_APP']) }}

WITH 
app AS (
	SELECT
	*
	FROM {{ source("pcd_owner","app")  }}),
organisation_unit AS (
	SELECT
	*
	FROM {{ source("pfs_owner","organisation_unit")  }}),
Oracle_Enterprise_0 AS (/* Formatted on 2008/03/03 16:37 (Formatter Plus v4.8.7) */ SELECT "RPT_ROW" FROM (SELECT 'D|' || CAST(mod_timestamp AS TEXT) || '|' || app_id || '|' || subtype_code || '|' || app_no || '|' || CAST(created_date AS TEXT) || '|' || created_by_staff_number || '|' || owner_by_staff_number || '|' || channel_cat_id || '|' || gl_dept_number || '|' || sm_case_id || '|' AS rpt_row FROM (SELECT app.mod_timestamp, app.app_id, app.subtype_code, app.app_no, app.created_date, (SELECT ou1.cba_staff_number FROM organisation_unit WHERE app.created_by_user_id = ou1.cs_user_id) AS created_by_staff_number, (SELECT ou2.cba_staff_number FROM organisation_unit WHERE app.owned_by_user_id = ou2.cs_user_id) AS owner_by_staff_number, app.channel_cat_id, (SELECT ou3.gl_dept_number FROM organisation_unit WHERE app.lodgement_branch_id = ou3.cs_premium_centre_id) AS gl_dept_number, app.sm_case_id FROM app WHERE app.subtype_code IN ('CLP') AND app.mod_timestamp BETWEEN STR_TO_DATE('{{ var("FBIZDATE") }}', '%Y%m%d%H%M%S') AND STR_TO_DATE('{{ var("TBIZDATE") }}', '%Y%m%d%H%M%S'))))


SELECT * FROM Oracle_Enterprise_0