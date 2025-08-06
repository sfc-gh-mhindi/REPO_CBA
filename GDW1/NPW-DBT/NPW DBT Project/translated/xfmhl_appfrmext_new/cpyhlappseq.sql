{{ config(materialized='view', tags=['XfmHL_APPFrmExt_NEW']) }}

WITH CpyHlAppSeq AS (
	SELECT
		{{ ref('SrcChlBusApp') }}.CHL_APP_HL_APP_ID AS HL_APP_ID,
		FOREIGN_INCOME_FLAG,
		FI_CURRENCY_CODE
	FROM {{ ref('SrcChlBusApp') }}
)

SELECT * FROM CpyHlAppSeq