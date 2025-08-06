{{ config(materialized='view', tags=['EXT_CLP_BUS_APP_PROD']) }}

WITH 
clp_app_prod AS (
	SELECT
	*
	FROM {{ source("pin_owner","clp_app_prod")  }}),
Oracle_Enterprise_0 AS (/* Formatted on 2008/03/05 11:30 (Formatter Plus v4.8.7) */ SELECT rpt_row FROM (SELECT 'D|' || CAST(mod_timestamp AS TEXT) || '|' || clp_app_prod_id || '|' || campaign_cat_id || '|' || loan_amt || '|' || insured_amt || '|' || loan_app_prod_id || '|' || loan_asset_liability_id || '|' || contract_no || '|' || payment_id || '|' || client_engagement_cat_id || '|' || clp_job_family_cat_id || '|' || client_staff_ou_id || '|' || question_submit_date || '|' || navigation_client_id || '|' || clp_sum_insured_cat_id || '|' || mod_user_id || '|' || ou_snap_id || '|' AS rpt_row FROM (SELECT cap.mod_timestamp, cap.clp_app_prod_id, cap.campaign_cat_id, cap.loan_amt, cap.insured_amt, cap.loan_app_prod_id, cap.loan_asset_liability_id, cap.contract_no, cap.payment_id, cap.client_engagement_cat_id, cap.clp_job_family_cat_id, cap.client_staff_ou_id, cap.question_submit_date, cap.navigation_client_id, cap.clp_sum_insured_cat_id, cap.mod_user_id, cap.ou_snap_id FROM clp_app_prod WHERE cap.mod_timestamp BETWEEN STR_TO_DATE('{{ var("FBIZDATE") }}', '%Y%m%d%H%M%S') AND STR_TO_DATE('{{ var("TBIZDATE") }}', '%Y%m%d%H%M%S'))))


SELECT * FROM Oracle_Enterprise_0