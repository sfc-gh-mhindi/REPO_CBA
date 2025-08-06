{{ config(materialized='view', tags=['DltUNID_PATY_GNRC_PRFLFrmTMP_UNID_PATY_GNRC_PRFL']) }}

WITH ChangeCapture AS (
	SELECT
		COALESCE({{ ref('CopyFil') }}.UNID_PATY_I, {{ ref('Copy') }}.UNID_PATY_I) AS UNID_PATY_I,
		CASE
			WHEN {{ ref('Copy') }}.UNID_PATY_I IS NULL THEN '1'
			WHEN {{ ref('CopyFil') }}.UNID_PATY_I IS NULL THEN '2'
			WHEN ({{ ref('Copy') }}.UNID_PATY_I = {{ ref('CopyFil') }}.UNID_PATY_I) AND ({{ ref('Copy') }}.GRDE_C <> {{ ref('CopyFil') }}.GRDE_C OR {{ ref('Copy') }}.SUB_GRDE_C <> {{ ref('CopyFil') }}.SUB_GRDE_C OR {{ ref('Copy') }}.PRNT_PRVG_F <> {{ ref('CopyFil') }}.PRNT_PRVG_F) THEN '3'
			WHEN {{ ref('Copy') }}.UNID_PATY_I = {{ ref('CopyFil') }}.UNID_PATY_I AND {{ ref('Copy') }}.GRDE_C = {{ ref('CopyFil') }}.GRDE_C AND {{ ref('Copy') }}.SUB_GRDE_C = {{ ref('CopyFil') }}.SUB_GRDE_C AND {{ ref('Copy') }}.PRNT_PRVG_F = {{ ref('CopyFil') }}.PRNT_PRVG_F THEN '0'
		END AS change_code 
	FROM {{ ref('Copy') }} 
	FULL OUTER JOIN {{ ref('CopyFil') }} 
	ON {{ ref('Copy') }}.UNID_PATY_I = {{ ref('CopyFil') }}.UNID_PATY_I
	WHERE change_code = '1' OR change_code = '3' OR change_code = '2'
)

SELECT * FROM ChangeCapture