{{ config(materialized='view', tags=['XfmDelFlagAPPT_TRNF_DETL']) }}

WITH XfmProsKey AS (
	SELECT
		{{ ref('TMP_DELETED') }}.DLTD_KEY1_VALU AS DELETED_KEY_1_VALUE,
		APPT_I,
		'CSE_CCC_BUS_APP_PROD_BAL_XFER_APPT_TRNF_DETL' AS CONV_M
	FROM {{ ref('TMP_DELETED') }}
	WHERE 
)

SELECT * FROM XfmProsKey