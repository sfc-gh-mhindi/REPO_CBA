{{ config(materialized='view', tags=['LdDelFlagREJT_COM_CPL_CCL_APP_PRODDel_2']) }}

SELECT
	COM_APP_PROD_ID 
FROM {{ ref('FnComAppProdId') }}