{{ config(materialized='view', tags=['ValidateBcFinsg']) }}

WITH 
_cba__app_ccods_uat_inbound_bcfinsg__c AS (
	SELECT
	*
	FROM {{ source("","_cba__app_ccods_uat_inbound_bcfinsg__c")  }})
BCFINSG AS (
	SELECT BCF_CORP_0,
		BCF_ACCOUNT_NO_0,
		BCF_PLAN_ID_0,
		BCF_PLAN_SEQ_0,
		BCF_DT_CURR_PROC,
		BCF_DT_NEXT_PROC,
		FILLER6
	FROM _cba__app_ccods_uat_inbound_bcfinsg__c*
)

SELECT * FROM BCFINSG