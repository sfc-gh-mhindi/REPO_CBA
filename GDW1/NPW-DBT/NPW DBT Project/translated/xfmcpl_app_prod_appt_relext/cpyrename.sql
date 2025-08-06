{{ config(materialized='view', tags=['XfmCPL_APP_PROD_Appt_RelExt']) }}

WITH CpyRename AS (
	SELECT
		{{ ref('SrcCCAppProdBalXferPremapDS') }}.LOAN_SUBTYPE_CODE AS LOAN_SBTY_CODE,
		APPT_I,
		{{ ref('SrcCCAppProdBalXferPremapDS') }}.REL_TYPE_C AS SBTY_CODE,
		RELD_APPT_I,
		ORIG_ETL_D
	FROM {{ ref('SrcCCAppProdBalXferPremapDS') }}
)

SELECT * FROM CpyRename