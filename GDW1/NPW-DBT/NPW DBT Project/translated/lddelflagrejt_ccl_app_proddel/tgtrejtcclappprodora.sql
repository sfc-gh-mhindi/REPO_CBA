{{ config(materialized='view', tags=['LdDelFlagREJT_CCL_APP_PRODDel']) }}

SELECT
	CCL_APP_PROD_ID 
FROM {{ ref('RenameCol') }}