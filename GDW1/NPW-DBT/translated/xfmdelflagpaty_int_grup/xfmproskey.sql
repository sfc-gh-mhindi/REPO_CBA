{{ config(materialized='view', tags=['XfmDelFlagPATY_INT_GRUP']) }}

WITH XfmProsKey AS (
	SELECT
		DELETED_TABLE_NAME,
		{{ ref('SrcPatyIntGrupDS') }}.DELETED_KEY_1_VALUE AS APP_PROD_ID,
		DELETED_KEY_2_VALUE,
		'CSE_COI_BUS_CLNT_UNDTAK_PATY_INT_GRUP' AS CONV_M
	FROM {{ ref('SrcPatyIntGrupDS') }}
	WHERE 
)

SELECT * FROM XfmProsKey