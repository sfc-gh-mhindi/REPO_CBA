{{ config(materialized='view', tags=['DltUNID_PATY_GNRC_PRFLFrmTMP_UNID_PATY_GNRC_PRFL']) }}

WITH Copy AS (
	SELECT
		UNID_PATY_I,
		GRDE_C,
		SUB_GRDE_C,
		PRNT_PRVG_F,
		EFFT_D
	FROM {{ ref('UnidPatyGnrcPrflVW') }}
)

SELECT * FROM Copy