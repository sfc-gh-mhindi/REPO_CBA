{{ config(materialized='view', tags=['DltUNID_PATY_GNRC_PRFLFrmTMP_UNID_PATY_GNRC_PRFL']) }}

WITH JoinToUpdates AS (
	SELECT
		{{ ref('Cpy') }}.UNID_PATY_I,
		{{ ref('Cpy') }}.EXPY_D,
		{{ ref('Cpy') }}.PROS_KEY_EXPY_I,
		{{ ref('Copy') }}.EFFT_D
	FROM {{ ref('Cpy') }}
	LEFT JOIN {{ ref('Copy') }} ON {{ ref('Cpy') }}.UNID_PATY_I = {{ ref('Copy') }}.UNID_PATY_I
)

SELECT * FROM JoinToUpdates