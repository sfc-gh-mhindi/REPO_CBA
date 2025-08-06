{{ config(materialized='view', tags=['XfmDelFlagAPPT_FEAT']) }}

WITH XfmProsKey AS (
	SELECT
		DELETED_TABLE_NAME,
		{{ ref('SrcAppFeatDS') }}.DELETED_KEY_1_VALUE AS APP_PROD_ID,
		DELETED_KEY_2_VALUE,
		'CSE_CCL_BUS_APP_FEE_APPT_FEAT' AS CONV_M
	FROM {{ ref('SrcAppFeatDS') }}
	WHERE 
)

SELECT * FROM XfmProsKey