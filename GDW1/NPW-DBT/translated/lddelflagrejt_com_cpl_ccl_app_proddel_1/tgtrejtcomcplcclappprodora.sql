{{ config(materialized='view', tags=['LdDelFlagREJT_COM_CPL_CCL_APP_PRODDel_1']) }}

SELECT
	CCL_APP_PROD_ID
	COM_APP_PROD_ID 
FROM {{ ref('Xfrm_RenameCol') }}