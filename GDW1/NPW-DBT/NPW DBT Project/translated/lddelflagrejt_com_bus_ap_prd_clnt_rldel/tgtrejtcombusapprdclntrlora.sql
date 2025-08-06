{{ config(materialized='view', tags=['LdDelFlagREJT_COM_BUS_AP_PRD_CLNT_RLDel']) }}

SELECT
	APP_PROD_ID
	CIF_CODE
	ROLE_CAT_ID 
FROM {{ ref('RenameCol') }}