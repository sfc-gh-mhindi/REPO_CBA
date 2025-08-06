{{ config(materialized='view', tags=['DltUNID_PATY_GNRC_PRFLFrmTMP_UNID_PATY_GNRC_PRFL']) }}

WITH JointoTx AS (
	SELECT
		{{ ref('ChangeCapture') }}.UNID_PATY_I,
		{{ ref('ChangeCapture') }}.change_code,
		{{ ref('CopyFil') }}.SRCE_SYST_C,
		{{ ref('CopyFil') }}.GRDE_C,
		{{ ref('CopyFil') }}.SUB_GRDE_C,
		{{ ref('CopyFil') }}.PRNT_PRVG_F,
		{{ ref('CopyFil') }}.EFFT_D,
		{{ ref('CopyFil') }}.EXPY_D,
		{{ ref('CopyFil') }}.PROS_KEY_EFFT_I,
		{{ ref('CopyFil') }}.PROS_KEY_EXPY_I,
		{{ ref('CopyFil') }}.ROW_SECU_ACCS_C
	FROM {{ ref('ChangeCapture') }}
	LEFT JOIN {{ ref('CopyFil') }} ON {{ ref('ChangeCapture') }}.UNID_PATY_I = {{ ref('CopyFil') }}.UNID_PATY_I
)

SELECT * FROM JointoTx