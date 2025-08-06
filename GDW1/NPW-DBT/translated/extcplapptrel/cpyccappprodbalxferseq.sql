{{ config(materialized='view', tags=['ExtCplApptRel']) }}

WITH CpyCCAppProdBalXferSeq AS (
	SELECT
		{{ ref('Data_Set_215') }}.SUBTYPE_CODE AS LOAN_SUBTYPE_CODE,
		{{ ref('Data_Set_215') }}.APP_ID AS APPT_I,
		{{ ref('Data_Set_215') }}.LOAN_SUBTYPE_CODE AS REL_TYPE_C,
		{{ ref('Data_Set_215') }}.LOAN_APP_ID AS RELD_APPT_I
	FROM {{ ref('Data_Set_215') }}
)

SELECT * FROM CpyCCAppProdBalXferSeq