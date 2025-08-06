{{ config(materialized='view', tags=['XfmDelFlagAPPT_PDCT_UNID_PATY']) }}

WITH XfmProsKey AS (
	SELECT
		DELETED_TABLE_NAME,
		DELETED_KEY_1_VALUE,
		'CSE_CPL_BUS_APP_PROD_APPT_PDCT_UNID_PATY' AS CONV_M
	FROM {{ ref('SrcApptPdctUnidPatyDS') }}
	WHERE 
)

SELECT * FROM XfmProsKey